//! Live integration tests for the schema bootstrap of the library.
//!
//! The tests need the fixture database of the tasks checkout. They are
//! ignored by default and run with
//! `CLOUDIFUL_TASKS_TEST_DATABASE_URL=<url> cargo test --test migration -- --include-ignored --test-threads=1`.

mod common;

use cloudiful_tasks::{EnqueueRequest, Error, TaskId, TaskKind, check_schema, migrate};
use sqlx::PgPool;
use sqlx::Row;

use common::{
    advisory_lock_count, cleanup, connect, create_empty_schema, create_foreign_schema,
    create_ledger_only_schema, create_public_sentinels, open_queue, relation_names, schema_exists,
};

const FAIR_ADMISSION_SEED_V2_SQL: &str = include_str!("../sql/tests/fair_admission_seed_v2.sql");

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

#[derive(sqlx::FromRow)]
struct FairAdmissionRow {
    class: Option<String>,
    resources: Vec<String>,
    attempt_count: i32,
    fencing_token: uuid::Uuid,
}

async fn seed_v2_task(pool: &PgPool) {
    sqlx::raw_sql(FAIR_ADMISSION_SEED_V2_SQL)
        .execute(pool)
        .await
        .expect("the representative old task must be insertable");
}

async fn assert_upgraded_task_metadata(pool: &PgPool) {
    let row = sqlx::query_file_as!(
        FairAdmissionRow,
        "sql/tests/fair_admission_state.sql",
        uuid::Uuid::from_u128(0x1111_1111_1111_1111_1111_1111_1111_1111),
    )
    .fetch_one(pool)
    .await
    .expect("the upgraded task must be readable");
    assert_eq!(row.class, None);
    assert!(row.resources.is_empty());
    assert_eq!(row.attempt_count, 1);
    assert_eq!(
        row.fencing_token,
        uuid::Uuid::from_u128(0x3333_3333_3333_3333_3333_3333_3333_3333)
    );

    let queue = open_queue(pool, "ns", "q").await;
    let replayed = queue
        .enqueue(
            EnqueueRequest::new(
                TaskKind::new("demo").unwrap(),
                serde_json::json!({"legacy": true}),
            )
            .with_idempotency_key("legacy-key")
            .unwrap(),
        )
        .await
        .expect("the stored v2 request specification must replay");
    assert!(!replayed.is_created());
    assert_eq!(
        replayed.task().id(),
        TaskId::from_uuid(uuid::Uuid::from_u128(
            0x1111_1111_1111_1111_1111_1111_1111_1111
        ))
    );
}

async fn remove_fair_admission_schema(pool: &PgPool, version: i64) {
    sqlx::raw_sql(
        "DROP INDEX cloudiful_tasks.tasks_claim_class_idx; \
         DROP INDEX cloudiful_tasks.tasks_resources_gin; \
         DROP TABLE cloudiful_tasks.queue_class_cursors; \
         DROP TABLE cloudiful_tasks.queue_resource_policies; \
         ALTER TABLE cloudiful_tasks.tasks \
             DROP COLUMN class, \
             DROP COLUMN resources",
    )
    .execute(pool)
    .await
    .expect("the fair admission schema fixture must be removable");
    sqlx::query("DELETE FROM cloudiful_tasks._sqlx_migrations WHERE version = $1")
        .bind(version)
        .execute(pool)
        .await
        .expect("the ledger row must be removable");
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
async fn an_owned_version_one_schema_is_upgraded() {
    let pool = connect().await;
    cleanup(&pool).await;
    let first = migrate(&pool)
        .await
        .expect("the first migration must succeed");
    let versions = first.applied_versions().to_vec();
    assert_eq!(versions.len(), 3);

    remove_fair_admission_schema(&pool, versions[2]).await;
    sqlx::query("DROP TABLE cloudiful_tasks.queue_policies")
        .execute(&pool)
        .await
        .expect("the policy table fixture must be removable");
    sqlx::query("DELETE FROM cloudiful_tasks._sqlx_migrations WHERE version = $1")
        .bind(versions[1])
        .execute(&pool)
        .await
        .expect("the version two ledger row must be removable");
    sqlx::query("UPDATE cloudiful_tasks.schema_ownership SET protocol_version = 1 WHERE singleton")
        .execute(&pool)
        .await
        .expect("the ownership marker must be writable");
    seed_v2_task(&pool).await;

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
    assert_eq!(upgraded.applied_versions(), first.applied_versions());
    let relations = relation_names(&pool, "cloudiful_tasks").await;
    assert!(relations.contains(&"queue_policies".to_owned()));
    assert!(relations.contains(&"queue_class_cursors".to_owned()));
    assert!(relations.contains(&"queue_resource_policies".to_owned()));
    assert_eq!(
        check_schema(&pool)
            .await
            .expect("the upgraded schema must be readable")
            .protocol_version(),
        3
    );
    assert_upgraded_task_metadata(&pool).await;
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn an_owned_version_two_schema_is_upgraded() {
    let pool = connect().await;
    cleanup(&pool).await;
    let first = migrate(&pool)
        .await
        .expect("the first migration must succeed");
    let versions = first.applied_versions().to_vec();
    assert_eq!(versions.len(), 3);

    remove_fair_admission_schema(&pool, versions[2]).await;
    sqlx::query("UPDATE cloudiful_tasks.schema_ownership SET protocol_version = 2 WHERE singleton")
        .execute(&pool)
        .await
        .expect("the ownership marker must be writable");
    seed_v2_task(&pool).await;

    let error = check_schema(&pool)
        .await
        .expect_err("an older protocol version must not be read");
    assert!(
        matches!(
            error,
            Error::OutdatedSchemaVersion {
                found: 2,
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
    assert_eq!(upgraded.applied_versions(), first.applied_versions());
    assert_eq!(
        check_schema(&pool)
            .await
            .expect("the upgraded schema must be readable")
            .protocol_version(),
        3
    );
    assert_upgraded_task_metadata(&pool).await;
}
