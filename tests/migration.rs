//! Live integration tests for the schema bootstrap of the library.
//!
//! The tests need the fixture database of the tasks checkout. They are
//! ignored by default and run with
//! `TASKS_TEST_DATABASE_URL=<url> cargo test --test migration -- --include-ignored --test-threads=1`.

mod common;

use cloudiful_tasks::{
    AttemptStatus, EnqueueRequest, Error, TaskId, TaskKind, TaskPauseState, TaskStatus,
    check_schema, migrate,
};
use sea_orm::{ConnectionTrait, DatabaseConnection};
use serde_json::json;
use sha2::{Digest, Sha384};

use sea_orm::sea_query::PostgresQueryBuilder;

use common::{
    advisory_lock_count, cleanup, connect, create_empty_schema, create_foreign_schema,
    create_ledger_only_schema, create_public_sentinels, fetch_scalar, open_queue, relation_names,
    schema_exists, statement,
};

const BUMP_PROTOCOL_VERSION_SQL: &str =
    include_str!("../sql/tests/migration_bump_protocol_version.sql");
const CORRUPT_CHECKSUM_SQL: &str = include_str!("../sql/tests/migration_corrupt_checksum.sql");
const PUBLIC_LEDGER_COUNT_SQL: &str =
    include_str!("../sql/tests/migration_public_ledger_count.sql");
const SENTINEL_COUNT_SQL: &str = include_str!("../sql/tests/migration_sentinel_count.sql");
const LEDGER_VERSIONS_SQL: &str = include_str!("../sql/tests/migration_ledger_versions.sql");
const LEDGER_SIGNATURE_SQL: &str = include_str!("../sql/tests/migration_ledger_signature.sql");
const LEGACY_LEDGER_INSERT_SQL: &str =
    include_str!("../sql/tests/migration_legacy_ledger_insert.sql");
const DIRTY_LEDGER_INSERT_SQL: &str =
    include_str!("../sql/tests/migration_dirty_ledger_insert.sql");
const LEGACY_V3_FIXTURE_SQL: &str = include_str!("../sql/tests/migration_legacy_v3_fixture.sql");
const QUEUE_POLICY_SET_SQL: &str = include_str!("../sql/store_queue_policy_set.sql");

/// The immutable historical migrations of the library, in application order.
const LEGACY_MIGRATIONS: &[(i64, &str, &str)] = &[
    (
        20261021032949,
        "init_schema",
        include_str!("../migrations/20261021032949_init_schema.sql"),
    ),
    (
        20261023175252,
        "queue_policies",
        include_str!("../migrations/20261023175252_queue_policies.sql"),
    ),
    (
        20261106211859,
        "task_pause",
        include_str!("../migrations/20261106211859_task_pause.sql"),
    ),
];

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn migrate_creates_only_the_owned_schema() {
    let pool = connect().await;
    cleanup(&pool).await;

    let outcome = migrate(&pool).await.expect("the migration must succeed");
    assert!(outcome.applied());
    assert_eq!(outcome.protocol_version(), 3);
    assert_eq!(outcome.applied_versions().len(), 3);
    assert!(!outcome.instance_uuid().is_nil());

    let owned = relation_names(&pool, "cloudiful_tasks").await;
    assert!(owned.contains(&"schema_ownership".to_owned()));
    assert!(owned.contains(&"tasks".to_owned()));
    assert!(owned.contains(&"attempts".to_owned()));
    assert!(owned.contains(&"queue_policies".to_owned()));
    assert!(owned.contains(&"_sqlx_migrations".to_owned()));

    let public = relation_names(&pool, "public").await;
    assert!(
        public.is_empty(),
        "the library must not create relations in the public schema: {public:?}"
    );

    let second = migrate(&pool)
        .await
        .expect("the migration must be idempotent");
    assert!(!second.applied());
    assert_eq!(second.instance_uuid(), outcome.instance_uuid());
    assert_eq!(second.applied_versions(), outcome.applied_versions());

    let info = check_schema(&pool)
        .await
        .expect("the schema must be compatible");
    assert_eq!(info.instance_uuid(), outcome.instance_uuid());
    assert_eq!(info.protocol_version(), 3);

    let pause_columns = fetch_scalar::<i64>(
        &pool,
        statement(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'cloudiful_tasks' \
               AND ((table_name = 'tasks' AND column_name IN ('pause_state', 'pause_generation', 'paused_attempts')) \
                 OR (table_name = 'attempts' AND column_name IN ('pause_generation', 'paused_at')))",
            Vec::new(),
        ),
    )
    .await
    .expect("the pause columns must be readable");
    assert_eq!(pause_columns, 5, "the pause columns must be present");

    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a finished migration must not leave a session lock behind"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn concurrent_migrations_are_idempotent() {
    let pool = connect().await;
    cleanup(&pool).await;

    let (first, second) = tokio::join!(migrate(&pool), migrate(&pool));
    let first = first.expect("the first migration must succeed");
    let second = second.expect("the second migration must succeed");

    assert_eq!(first.instance_uuid(), second.instance_uuid());
    assert_eq!(first.applied_versions(), second.applied_versions());

    let versions = pool
        .query_all_raw(statement(LEDGER_VERSIONS_SQL, Vec::new()))
        .await
        .expect("the migration ledger must be readable");
    assert_eq!(versions.len(), 3);
    let first_version = versions[0]
        .try_get_by::<i64, usize>(0)
        .expect("the migration version must decode");
    assert_eq!(first_version, first.applied_versions()[0]);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_foreign_schema_is_refused() {
    let pool = connect().await;
    cleanup(&pool).await;
    create_foreign_schema(&pool).await;

    let error = migrate(&pool)
        .await
        .expect_err("a schema that the library does not own must be refused");
    assert!(
        matches!(error, Error::ForeignSchema { .. }),
        "unexpected error: {error}"
    );

    let error = check_schema(&pool)
        .await
        .expect_err("a schema that the library does not own must be refused");
    assert!(
        matches!(error, Error::ForeignSchema { .. }),
        "unexpected error: {error}"
    );

    let owned = relation_names(&pool, "cloudiful_tasks").await;
    assert!(
        owned.contains(&"foreign_tasks".to_owned()),
        "the foreign schema must not be adopted: {owned:?}"
    );
    assert!(
        !owned.contains(&"tasks".to_owned()),
        "the foreign schema must not be adopted: {owned:?}"
    );

    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_empty_schema_is_refused() {
    let pool = connect().await;
    cleanup(&pool).await;
    create_empty_schema(&pool).await;
    assert!(
        schema_exists(&pool, "cloudiful_tasks").await,
        "the fixture schema must exist"
    );

    let error = migrate(&pool)
        .await
        .expect_err("an empty schema must not be adopted");
    assert!(
        matches!(error, Error::ForeignSchema { .. }),
        "unexpected error: {error}"
    );

    let owned = relation_names(&pool, "cloudiful_tasks").await;
    assert!(
        owned.is_empty(),
        "the schema must stay untouched: {owned:?}"
    );

    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_schema_with_the_ledger_alone_is_refused() {
    let pool = connect().await;
    cleanup(&pool).await;
    create_ledger_only_schema(&pool).await;

    let error = migrate(&pool)
        .await
        .expect_err("a schema with the ledger alone must not be adopted");
    assert!(
        matches!(error, Error::ForeignSchema { .. }),
        "unexpected error: {error}"
    );

    let owned = relation_names(&pool, "cloudiful_tasks").await;
    assert_eq!(
        owned,
        vec!["_sqlx_migrations".to_owned()],
        "the schema must stay untouched: {owned:?}"
    );

    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_missing_schema_is_reported_and_never_created() {
    let pool = connect().await;
    cleanup(&pool).await;

    let error = check_schema(&pool)
        .await
        .expect_err("a missing schema must be reported");
    assert!(
        matches!(error, Error::SchemaMissing { .. }),
        "unexpected error: {error}"
    );

    assert!(
        !schema_exists(&pool, "cloudiful_tasks").await,
        "the check must not create the schema"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_newer_protocol_version_is_refused() {
    let pool = connect().await;
    cleanup(&pool).await;
    migrate(&pool).await.expect("the migration must succeed");

    pool.execute_unprepared(BUMP_PROTOCOL_VERSION_SQL)
        .await
        .expect("the marker must be writable");

    let error = check_schema(&pool)
        .await
        .expect_err("a newer protocol version must be refused");
    assert!(
        matches!(
            error,
            Error::UnsupportedSchemaVersion {
                found: 999,
                supported: 3
            }
        ),
        "unexpected error: {error}"
    );

    let error = migrate(&pool)
        .await
        .expect_err("a newer protocol version must be refused");
    assert!(
        matches!(
            error,
            Error::UnsupportedSchemaVersion {
                found: 999,
                supported: 3
            }
        ),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_drifted_migration_checksum_is_detected() {
    let pool = connect().await;
    cleanup(&pool).await;
    migrate(&pool).await.expect("the migration must succeed");

    pool.execute_unprepared(CORRUPT_CHECKSUM_SQL)
        .await
        .expect("the ledger must be writable");

    let error = check_schema(&pool)
        .await
        .expect_err("a drifted checksum must be detected");
    assert!(
        matches!(error, Error::ChecksumMismatch { .. }),
        "unexpected error: {error}"
    );

    let error = migrate(&pool)
        .await
        .expect_err("a drifted checksum must be detected");
    assert!(
        matches!(error, Error::Migration(_)),
        "unexpected error: {error}"
    );

    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a failed migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn the_public_schema_is_untouched() {
    let pool = connect().await;
    cleanup(&pool).await;
    create_public_sentinels(&pool).await;

    let before = relation_names(&pool, "public").await;
    migrate(&pool).await.expect("the migration must succeed");
    let after = relation_names(&pool, "public").await;

    assert_eq!(
        before, after,
        "the migration must not touch the public schema"
    );
    assert!(after.contains(&"sentinel_ledger".to_owned()));
    assert!(after.contains(&"_sqlx_migrations".to_owned()));

    let sentinels = fetch_scalar::<i64>(&pool, statement(SENTINEL_COUNT_SQL, Vec::new()))
        .await
        .expect("the sentinel fixture must be readable");
    assert_eq!(sentinels, 1);

    let ledger = fetch_scalar::<i64>(&pool, statement(PUBLIC_LEDGER_COUNT_SQL, Vec::new()))
        .await
        .expect("the public ledger must be readable");
    assert_eq!(
        ledger, 0,
        "the library must not write to the public migration ledger"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn the_migration_runs_on_an_empty_database() {
    let pool: DatabaseConnection = connect().await;
    cleanup(&pool).await;

    let outcome = migrate(&pool).await.expect("the migration must succeed");
    assert!(outcome.applied());
    assert_eq!(outcome.applied_versions().len(), 3);
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_populated_version_two_schema_is_upgraded() {
    let pool = connect().await;
    cleanup(&pool).await;
    let complete = migrate(&pool).await.expect("the migration must succeed");
    let applied = complete.applied_versions().to_vec();
    let pause_version = *applied.last().expect("the pause migration must be applied");

    // Restore a version two schema: remove the pause objects and trim the
    // ledger and the ownership marker.
    downgrade_to_version_two(&pool, pause_version).await;

    let id = uuid::Uuid::new_v4();
    let attempt_id = uuid::Uuid::new_v4();
    pool.execute_raw(statement(
        "INSERT INTO cloudiful_tasks.tasks (id, namespace, queue, kind, payload, status, request_spec, attempt_count, max_attempts) VALUES ($1, 'ns', 'q', 'demo', $2, 'running', $2, 1, 3)",
        vec![id.into(), json!({"stage": "saved"}).into()],
    ))
    .await
    .unwrap();
    pool.execute_raw(statement(
        "INSERT INTO cloudiful_tasks.attempts (id, task_id, attempt_number, worker_id, fencing_token, status, lease_expires_at) VALUES ($1, $2, 1, 'worker', $3, 'running', now() + interval '5 minutes')",
        vec![
            attempt_id.into(),
            id.into(),
            uuid::Uuid::new_v4().into(),
        ],
    ))
    .await
    .unwrap();
    pool.execute_raw(statement(
        QUEUE_POLICY_SET_SQL,
        vec!["ns".into(), "q".into(), 2_i32.into()],
    ))
    .await
    .unwrap();
    let upgraded = migrate(&pool).await.unwrap();
    assert_eq!(upgraded.protocol_version(), 3);
    assert!(upgraded.applied());
    let queue = open_queue(&pool, "ns", "q").await;
    assert_eq!(
        queue.policy().await.unwrap().max_concurrent_attempts,
        Some(2)
    );
    let task = queue
        .get(cloudiful_tasks::TaskId::from_uuid(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(task.payload(), &json!({"stage": "saved"}));
    assert_eq!(task.status(), TaskStatus::Running);
    assert_eq!(task.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(task.pause_generation(), 0);
    assert_eq!(task.paused_attempts(), 0);
    let attempts = queue.attempts(task.id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].id().as_uuid(), attempt_id);
    assert_eq!(attempts[0].status(), AttemptStatus::Running);
    assert_eq!(attempts[0].pause_generation(), 0);
    assert!(attempts[0].paused_at().is_none());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_owned_version_one_schema_is_upgraded() {
    let pool = connect().await;
    cleanup(&pool).await;
    let first = migrate(&pool)
        .await
        .expect("the first migration must succeed");
    let applied = first.applied_versions().to_vec();
    assert_eq!(applied.len(), 3);

    // Seed one queued task and one claimed attempt before the downgrade. The
    // rows must survive the upgrade with the additive defaults.
    let queue = open_queue(&pool, "ns", "q").await;
    let queued = queue
        .enqueue(EnqueueRequest::new(
            TaskKind::new("demo").unwrap(),
            json!({"stage": "queued"}),
        ))
        .await
        .unwrap();
    let running = queue
        .enqueue(EnqueueRequest::new(
            TaskKind::new("demo").unwrap(),
            json!({"stage": "running"}),
        ))
        .await
        .unwrap();
    let claim = queue
        .claim_by_id(running.task().id(), "worker")
        .await
        .unwrap()
        .expect("the running task must be claimed");

    // Simulate an owned protocol version 1 schema: remove the version 2 and
    // version 3 objects, restore the version 1 attempt status check and trim
    // the ledger.
    let downgrade = format!(
        "ALTER TABLE cloudiful_tasks.attempts \
             DROP CONSTRAINT attempts_paused_at_check, \
             DROP CONSTRAINT attempts_pause_generation_check, \
             DROP CONSTRAINT attempts_status_check, \
             ADD CONSTRAINT attempts_status_check \
                 CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted')), \
             DROP COLUMN paused_at, \
             DROP COLUMN pause_generation; \
         ALTER TABLE cloudiful_tasks.tasks \
             DROP CONSTRAINT tasks_paused_attempts_check, \
             DROP CONSTRAINT tasks_pause_generation_check, \
             DROP CONSTRAINT tasks_pause_state_check, \
             DROP COLUMN paused_attempts, \
             DROP COLUMN pause_generation, \
             DROP COLUMN pause_state; \
         DROP TABLE cloudiful_tasks.queue_policies; \
         DELETE FROM cloudiful_tasks._sqlx_migrations WHERE version > {}; \
         UPDATE cloudiful_tasks.schema_ownership SET protocol_version = 1 WHERE singleton;",
        applied[0]
    );
    pool.execute_unprepared(&downgrade)
        .await
        .expect("the version one fixture must be restorable");

    let error = check_schema(&pool)
        .await
        .expect_err("an older protocol version must not be read");
    assert!(
        matches!(
            error,
            Error::OutdatedSchemaVersion {
                found: 1,
                supported: 3
            }
        ),
        "unexpected error: {error}"
    );

    let upgraded = migrate(&pool)
        .await
        .expect("an owned older schema must be upgraded");
    assert!(upgraded.applied());
    assert_eq!(upgraded.protocol_version(), 3);
    assert_eq!(upgraded.applied_versions(), applied);
    assert!(
        relation_names(&pool, "cloudiful_tasks")
            .await
            .contains(&"queue_policies".to_owned()),
        "the upgrade must create the policy table"
    );
    assert_eq!(
        check_schema(&pool)
            .await
            .expect("the upgraded schema must be readable")
            .protocol_version(),
        3
    );

    // The seeded rows survive with the additive pause defaults.
    let queued = queue
        .get(queued.task().id())
        .await
        .unwrap()
        .expect("the queued task must survive");
    assert_eq!(queued.status(), TaskStatus::Queued);
    assert_eq!(queued.pause_state(), TaskPauseState::Unpaused);
    assert_eq!(queued.pause_generation(), 0);
    assert_eq!(queued.paused_attempts(), 0);

    let running = queue
        .get(running.task().id())
        .await
        .unwrap()
        .expect("the running task must survive");
    assert_eq!(running.status(), TaskStatus::Running);
    assert_eq!(running.pause_state(), TaskPauseState::Unpaused);

    let attempts = queue.attempts(claim.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Running);
    assert_eq!(attempts[0].pause_generation(), 0);
    assert!(attempts[0].paused_at().is_none());
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_version_three_sqlx_fixture_is_preserved() {
    let pool = connect().await;
    cleanup(&pool).await;
    seed_legacy_version_three(&pool).await;

    let before = ledger_signature(&pool).await;
    assert_eq!(before.len(), LEGACY_MIGRATIONS.len());
    for (row, (version, description, sql)) in before.iter().zip(LEGACY_MIGRATIONS) {
        assert_eq!(row.0, *version);
        assert_eq!(row.1, *description);
        assert!(row.2);
        assert_eq!(row.3, hex_checksum(sql));
    }

    let outcome = migrate(&pool)
        .await
        .expect("an up to date legacy schema must migrate without a write");
    assert!(!outcome.applied());
    assert_eq!(outcome.protocol_version(), 3);
    assert_eq!(outcome.applied_versions().len(), LEGACY_MIGRATIONS.len());
    assert_eq!(
        ledger_signature(&pool).await,
        before,
        "the legacy ledger must stay unchanged"
    );

    let queue = open_queue(&pool, "legacy", "q").await;
    let paused = queue
        .get(TaskId::from_uuid(
            uuid::Uuid::parse_str("11111111-1111-4111-8111-111111111111").unwrap(),
        ))
        .await
        .unwrap()
        .expect("the paused task must survive");
    assert_eq!(paused.status(), TaskStatus::Queued);
    assert_eq!(paused.pause_state(), TaskPauseState::Paused);
    assert_eq!(paused.pause_generation(), 2);
    assert_eq!(paused.paused_attempts(), 1);
    assert_eq!(paused.payload(), &json!({"stage": "paused"}));

    let running = queue
        .get(TaskId::from_uuid(
            uuid::Uuid::parse_str("33333333-3333-4333-8333-333333333333").unwrap(),
        ))
        .await
        .unwrap()
        .expect("the running task must survive");
    assert_eq!(running.status(), TaskStatus::Running);
    assert_eq!(running.pause_state(), TaskPauseState::Pausing);

    let attempts = queue.attempts(running.id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Running);
    assert_eq!(attempts[0].pause_generation(), 1);

    assert_eq!(
        queue.policy().await.unwrap().max_concurrent_attempts,
        Some(3)
    );

    let ready_id =
        TaskId::from_uuid(uuid::Uuid::parse_str("22222222-2222-4222-8222-222222222222").unwrap());
    let claim = queue
        .claim_by_id(ready_id, "legacy-worker")
        .await
        .unwrap()
        .expect("the ready task must be claimable");
    queue
        .complete(claim.attempt(), Some(json!({"legacy": true})))
        .await
        .unwrap();
    let completed = queue.get(ready_id).await.unwrap().unwrap();
    assert_eq!(completed.status(), TaskStatus::Succeeded);
    assert_eq!(completed.result(), Some(&json!({"legacy": true})));

    let second = migrate(&pool)
        .await
        .expect("a repeated migration must stay a no-op");
    assert!(!second.applied());
    assert_eq!(
        ledger_signature(&pool).await,
        before,
        "task operations and a repeated migration must not rewrite the ledger"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_unknown_applied_migration_is_refused() {
    let pool = connect().await;
    cleanup(&pool).await;
    migrate(&pool).await.expect("the migration must succeed");

    let version = 99_999_999_999_999_i64;
    pool.execute_raw(statement(
        LEGACY_LEDGER_INSERT_SQL,
        vec![
            version.into(),
            "future_migration".into(),
            vec![0_u8; 48].into(),
            1_i64.into(),
        ],
    ))
    .await
    .expect("an unknown ledger row must be insertable");

    let error = check_schema(&pool)
        .await
        .expect_err("an unknown applied migration must be refused");
    assert!(
        matches!(error, Error::UnknownAppliedMigration { version: found } if found == version),
        "unexpected error: {error}"
    );

    let error = migrate(&pool)
        .await
        .expect_err("an unknown applied migration must be refused");
    assert!(
        matches!(error, Error::Migration(_)),
        "unexpected error: {error}"
    );

    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn an_unknown_dirty_migration_is_refused_without_changes() {
    let pool = connect().await;
    cleanup(&pool).await;
    migrate(&pool).await.expect("the migration must succeed");

    let version = 99_999_999_999_998_i64;
    pool.execute_raw(statement(
        DIRTY_LEDGER_INSERT_SQL,
        vec![
            version.into(),
            "future_migration".into(),
            vec![0_u8; 48].into(),
            1_i64.into(),
        ],
    ))
    .await
    .expect("an unsuccessful ledger row must be insertable");
    let before = ledger_signature(&pool).await;

    let error = check_schema(&pool)
        .await
        .expect_err("a dirty migration must not be reported as ready");
    assert!(
        matches!(error, Error::DirtyMigration { version: found } if found == version),
        "unexpected error: {error}"
    );

    let error = migrate(&pool)
        .await
        .expect_err("a dirty migration must be refused");
    assert!(
        matches!(error, Error::Migration(_)),
        "unexpected error: {error}"
    );

    assert_eq!(
        ledger_signature(&pool).await,
        before,
        "a refused dirty migration must not change the ledger"
    );
    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn a_dirty_known_migration_is_refused_without_retrying() {
    let pool = connect().await;
    cleanup(&pool).await;
    let complete = migrate(&pool).await.expect("the migration must succeed");
    let pause_version = *complete
        .applied_versions()
        .last()
        .expect("the pause migration must be applied");

    pool.execute_raw(statement(
        "UPDATE cloudiful_tasks._sqlx_migrations SET success = false WHERE version = $1",
        vec![pause_version.into()],
    ))
    .await
    .expect("the applied migration must be markable as dirty");
    let before = ledger_signature(&pool).await;

    let error = check_schema(&pool)
        .await
        .expect_err("a dirty known migration must not be reported as ready");
    assert!(
        matches!(error, Error::DirtyMigration { version: found } if found == pause_version),
        "unexpected error: {error}"
    );

    let error = migrate(&pool)
        .await
        .expect_err("a dirty known migration must be refused");
    assert!(
        matches!(error, Error::Migration(_)),
        "unexpected error: {error}"
    );
    assert_eq!(
        ledger_signature(&pool).await,
        before,
        "a refused dirty migration must not change the ledger"
    );
    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );

    // A dirty row for a known but unapplied migration must not be retried.
    cleanup(&pool).await;
    let complete = migrate(&pool).await.expect("the migration must succeed");
    let pause_version = *complete
        .applied_versions()
        .last()
        .expect("the pause migration must be applied");
    downgrade_to_version_two(&pool, pause_version).await;
    let pause = LEGACY_MIGRATIONS
        .iter()
        .find(|(version, _, _)| *version == pause_version)
        .expect("the pause migration must be known");
    pool.execute_raw(statement(
        DIRTY_LEDGER_INSERT_SQL,
        vec![
            pause_version.into(),
            pause.1.into(),
            Sha384::digest(pause.2.as_bytes()).to_vec().into(),
            7_i64.into(),
        ],
    ))
    .await
    .expect("the dirty known ledger row must be insertable");
    let before = ledger_signature(&pool).await;

    let error = migrate(&pool)
        .await
        .expect_err("a dirty unapplied migration must not be retried");
    assert!(
        matches!(error, Error::Migration(_)),
        "unexpected error: {error}"
    );
    assert_eq!(
        ledger_signature(&pool).await,
        before,
        "the dirty row must stay unchanged"
    );
    assert_eq!(
        fetch_scalar::<i32>(
            &pool,
            statement(
                "SELECT protocol_version FROM cloudiful_tasks.schema_ownership WHERE singleton",
                Vec::new(),
            ),
        )
        .await,
        Some(2),
        "the refused migration must not advance the protocol"
    );
    assert_eq!(
        fetch_scalar::<i64>(
            &pool,
            statement(
                "SELECT count(*) FROM information_schema.columns \
                 WHERE table_schema = 'cloudiful_tasks' AND table_name = 'attempts' \
                   AND column_name = 'paused_at'",
                Vec::new(),
            ),
        )
        .await,
        Some(0),
        "the refused migration must not apply the pause columns"
    );
    assert_eq!(
        advisory_lock_count(&pool).await,
        0,
        "a refused migration must not leak a session lock into the pool"
    );
}

#[tokio::test]
#[ignore = "requires TASKS_TEST_DATABASE_URL"]
async fn native_schema_declarations_match_the_migrated_schema() {
    const NATIVE: &str = "cloudiful_tasks_native_check";
    let pool = connect().await;
    cleanup(&pool).await;
    pool.execute_unprepared(&format!(
        "DROP SCHEMA IF EXISTS {NATIVE} CASCADE; CREATE SCHEMA {NATIVE};"
    ))
    .await
    .expect("the native declaration schema must be creatable");
    migrate(&pool).await.expect("the migration must succeed");

    for table in [
        cloudiful_tasks::schema::schema_ownership(NATIVE),
        cloudiful_tasks::schema::tasks(NATIVE),
        cloudiful_tasks::schema::attempts(NATIVE),
        cloudiful_tasks::schema::queue_policies(NATIVE),
    ] {
        pool.execute_unprepared(&table.to_string(PostgresQueryBuilder))
            .await
            .expect("the native table declaration must apply");
    }
    for index in cloudiful_tasks::schema::indexes(NATIVE) {
        pool.execute_unprepared(&index.to_string(PostgresQueryBuilder))
            .await
            .expect("the native index declaration must apply");
    }

    let columns = |schema: &'static str| {
        let pool = pool.clone();
        async move {
            pool.query_all_raw(statement(
                "SELECT table_name, column_name, data_type, udt_name, is_nullable, \
                        coalesce(column_default, '') \
                 FROM information_schema.columns \
                 WHERE table_schema = $1 \
                   AND table_name IN ('schema_ownership', 'tasks', 'attempts', 'queue_policies') \
                 ORDER BY table_name, ordinal_position",
                vec![schema.into()],
            ))
            .await
            .expect("the columns must be readable")
            .into_iter()
            .map(|row| {
                (
                    row.try_get_by::<String, usize>(0).unwrap(),
                    row.try_get_by::<String, usize>(1).unwrap(),
                    row.try_get_by::<String, usize>(2).unwrap(),
                    row.try_get_by::<String, usize>(3).unwrap(),
                    row.try_get_by::<String, usize>(4).unwrap(),
                    row.try_get_by::<String, usize>(5).unwrap(),
                )
            })
            .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        columns("cloudiful_tasks").await,
        columns(NATIVE).await,
        "the native columns must match the migrated columns"
    );

    let constraints = |schema: &'static str| {
        let pool = pool.clone();
        async move {
            pool.query_all_raw(statement(
                "SELECT c.relname, con.conname, con.contype::text, \
                        pg_get_constraintdef(con.oid) \
                 FROM pg_constraint AS con \
                 INNER JOIN pg_class AS c ON c.oid = con.conrelid \
                 INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 \
                   AND c.relname IN ('schema_ownership', 'tasks', 'attempts', 'queue_policies') \
                 ORDER BY c.relname, con.conname",
                vec![schema.into()],
            ))
            .await
            .expect("the constraints must be readable")
            .into_iter()
            .map(|row| {
                (
                    row.try_get_by::<String, usize>(0).unwrap(),
                    row.try_get_by::<String, usize>(1).unwrap(),
                    row.try_get_by::<String, usize>(2).unwrap(),
                    row.try_get_by::<String, usize>(3)
                        .unwrap()
                        .replace(NATIVE, "cloudiful_tasks"),
                )
            })
            .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        constraints("cloudiful_tasks").await,
        constraints(NATIVE).await,
        "the native constraints must match the migrated constraints"
    );

    let indexes = |schema: &'static str| {
        let pool = pool.clone();
        async move {
            pool.query_all_raw(statement(
                "SELECT tablename, indexname, indexdef FROM pg_indexes \
                 WHERE schemaname = $1 \
                   AND tablename IN ('schema_ownership', 'tasks', 'attempts', 'queue_policies') \
                 ORDER BY tablename, indexname",
                vec![schema.into()],
            ))
            .await
            .expect("the indexes must be readable")
            .into_iter()
            .map(|row| {
                (
                    row.try_get_by::<String, usize>(0).unwrap(),
                    row.try_get_by::<String, usize>(1).unwrap(),
                    row.try_get_by::<String, usize>(2)
                        .unwrap()
                        .replace(NATIVE, "cloudiful_tasks"),
                )
            })
            .collect::<Vec<_>>()
        }
    };
    assert_eq!(
        indexes("cloudiful_tasks").await,
        indexes(NATIVE).await,
        "the native indexes must match the migrated indexes"
    );

    pool.execute_unprepared(&format!("DROP SCHEMA {NATIVE} CASCADE;"))
        .await
        .expect("the native declaration schema must be removable");
}

async fn downgrade_to_version_two(pool: &DatabaseConnection, pause_version: i64) {
    let downgrade = format!(
        "ALTER TABLE cloudiful_tasks.attempts \
             DROP CONSTRAINT attempts_paused_at_check, \
             DROP CONSTRAINT attempts_pause_generation_check, \
             DROP CONSTRAINT attempts_status_check, \
             ADD CONSTRAINT attempts_status_check \
                 CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted')), \
             DROP COLUMN paused_at, \
             DROP COLUMN pause_generation; \
         ALTER TABLE cloudiful_tasks.tasks \
             DROP CONSTRAINT tasks_paused_attempts_check, \
             DROP CONSTRAINT tasks_pause_generation_check, \
             DROP CONSTRAINT tasks_pause_state_check, \
             DROP COLUMN paused_attempts, \
             DROP COLUMN pause_generation, \
             DROP COLUMN pause_state; \
         DELETE FROM cloudiful_tasks._sqlx_migrations WHERE version = {pause_version}; \
         UPDATE cloudiful_tasks.schema_ownership SET protocol_version = 2 WHERE singleton;"
    );
    pool.execute_unprepared(&downgrade)
        .await
        .expect("the version two fixture must be restorable");
}

async fn seed_legacy_version_three(pool: &DatabaseConnection) {
    for (_, _, sql) in LEGACY_MIGRATIONS {
        pool.execute_unprepared(sql)
            .await
            .expect("the immutable historical migration must apply");
    }
    pool.execute_unprepared(include_str!("../sql/migration_create_ledger.sql"))
        .await
        .expect("the legacy ledger must be creatable");
    for (version, description, sql) in LEGACY_MIGRATIONS {
        pool.execute_raw(statement(
            LEGACY_LEDGER_INSERT_SQL,
            vec![
                (*version).into(),
                (*description).into(),
                Sha384::digest(sql.as_bytes()).to_vec().into(),
                7_i64.into(),
            ],
        ))
        .await
        .expect("the legacy ledger row must be insertable");
    }
    pool.execute_unprepared(LEGACY_V3_FIXTURE_SQL)
        .await
        .expect("the version three fixture must be insertable");
}

async fn ledger_signature(
    pool: &DatabaseConnection,
) -> Vec<(i64, String, bool, String, i64, String)> {
    pool.query_all_raw(statement(LEDGER_SIGNATURE_SQL, Vec::new()))
        .await
        .expect("the legacy ledger must be readable")
        .into_iter()
        .map(|row| {
            (
                row.try_get_by::<i64, usize>(0).unwrap(),
                row.try_get_by::<String, usize>(1).unwrap(),
                row.try_get_by::<bool, usize>(2).unwrap(),
                row.try_get_by::<String, usize>(3).unwrap(),
                row.try_get_by::<i64, usize>(4).unwrap(),
                row.try_get_by::<String, usize>(5).unwrap(),
            )
        })
        .collect()
}

fn hex_checksum(sql: &str) -> String {
    Sha384::digest(sql.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
