//! Live integration tests for the schema bootstrap of the library.
//!
//! The tests need the fixture database of the tasks checkout. They are
//! ignored by default and run with
//! `CLOUDIFUL_TASKS_TEST_DATABASE_URL=<url> cargo test --test migration -- --include-ignored --test-threads=1`.

mod common;

use cloudiful_tasks::{
    AttemptStatus, EnqueueRequest, Error, TaskKind, TaskPauseState, TaskStatus, check_schema,
    migrate,
};
use serde_json::json;
use sqlx::PgPool;
use sqlx::Row;

use common::{
    advisory_lock_count, cleanup, connect, create_empty_schema, create_foreign_schema,
    create_ledger_only_schema, create_public_sentinels, open_queue, relation_names, schema_exists,
};

const BUMP_PROTOCOL_VERSION_SQL: &str =
    include_str!("../sql/tests/migration_bump_protocol_version.sql");
const CORRUPT_CHECKSUM_SQL: &str = include_str!("../sql/tests/migration_corrupt_checksum.sql");
const PUBLIC_LEDGER_COUNT_SQL: &str =
    include_str!("../sql/tests/migration_public_ledger_count.sql");
const SENTINEL_COUNT_SQL: &str = include_str!("../sql/tests/migration_sentinel_count.sql");

#[derive(sqlx::FromRow)]
struct LedgerVersionRow {
    migration_version: i64,
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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

    let pause_columns = sqlx::query_scalar::<_, i64>(
        "SELECT count(*) FROM information_schema.columns \
         WHERE table_schema = 'cloudiful_tasks' \
           AND ((table_name = 'tasks' AND column_name IN ('pause_state', 'pause_generation', 'paused_attempts')) \
             OR (table_name = 'attempts' AND column_name IN ('pause_generation', 'paused_at')))",
    )
    .fetch_one(&pool)
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn concurrent_migrations_are_idempotent() {
    let pool = connect().await;
    cleanup(&pool).await;

    let (first, second) = tokio::join!(migrate(&pool), migrate(&pool));
    let first = first.expect("the first migration must succeed");
    let second = second.expect("the second migration must succeed");

    assert_eq!(first.instance_uuid(), second.instance_uuid());
    assert_eq!(first.applied_versions(), second.applied_versions());

    let versions =
        sqlx::query_file_as!(LedgerVersionRow, "sql/tests/migration_ledger_versions.sql")
            .fetch_all(&pool)
            .await
            .expect("the migration ledger must be readable");
    assert_eq!(versions.len(), 3);
    assert_eq!(versions[0].migration_version, first.applied_versions()[0]);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_newer_protocol_version_is_refused() {
    let pool = connect().await;
    cleanup(&pool).await;
    migrate(&pool).await.expect("the migration must succeed");

    sqlx::raw_sql(BUMP_PROTOCOL_VERSION_SQL)
        .execute(&pool)
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_drifted_migration_checksum_is_detected() {
    let pool = connect().await;
    cleanup(&pool).await;
    migrate(&pool).await.expect("the migration must succeed");

    sqlx::raw_sql(CORRUPT_CHECKSUM_SQL)
        .execute(&pool)
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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

    let sentinels = sqlx::raw_sql(SENTINEL_COUNT_SQL)
        .fetch_one(&pool)
        .await
        .expect("the sentinel fixture must be readable");
    assert_eq!(sentinels.try_get::<i64, _>("sentinel_entries").unwrap(), 1);

    let ledger = sqlx::raw_sql(PUBLIC_LEDGER_COUNT_SQL)
        .fetch_one(&pool)
        .await
        .expect("the public ledger must be readable");
    assert_eq!(
        ledger.try_get::<i64, _>("ledger_entries").unwrap(),
        0,
        "the library must not write to the public migration ledger"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_migration_runs_on_an_empty_database() {
    let pool: PgPool = connect().await;
    cleanup(&pool).await;

    let outcome = migrate(&pool).await.expect("the migration must succeed");
    assert!(outcome.applied());
    assert_eq!(outcome.applied_versions().len(), 3);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_populated_version_two_schema_is_upgraded() {
    let pool = connect().await;
    cleanup(&pool).await;
    let mut old = sqlx::migrate!("./migrations");
    old.migrations = std::borrow::Cow::Owned(old.migrations.iter().take(2).cloned().collect());
    old.run(&pool).await.unwrap();
    let id = uuid::Uuid::new_v4();
    let attempt_id = uuid::Uuid::new_v4();
    sqlx::query("INSERT INTO cloudiful_tasks.tasks (id, namespace, queue, kind, payload, status, request_spec, attempt_count, max_attempts) VALUES ($1, 'ns', 'q', 'demo', $2, 'running', $2, 1, 3)")
        .bind(id).bind(json!({"stage": "saved"})).execute(&pool).await.unwrap();
    sqlx::query("INSERT INTO cloudiful_tasks.attempts (id, task_id, attempt_number, worker_id, fencing_token, status, lease_expires_at) VALUES ($1, $2, 1, 'worker', $3, 'running', now() + interval '5 minutes')")
        .bind(attempt_id).bind(id).bind(uuid::Uuid::new_v4()).execute(&pool).await.unwrap();
    sqlx::query_file!("sql/store_queue_policy_set.sql", "ns", "q", Some(2_i32))
        .execute(&pool)
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
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
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
    sqlx::raw_sql(sqlx::AssertSqlSafe(downgrade))
        .execute(&pool)
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
