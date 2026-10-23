//! Shared helpers for the live integration tests.
//!
//! The helpers fail the test when the test database is missing or does not
//! match the expected fixture identity. A live test never reports success
//! without a verified database.

// The helpers are shared by the test binaries, not every binary uses all of
// them.
#![allow(dead_code)]

use std::str::FromStr;
use std::time::Duration;

use cloudiful_tasks::{Queue, WorkerConfig};
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};

/// Environment variable that holds the URL of the dedicated test database.
pub const TEST_DATABASE_URL_ENV: &str = "CLOUDIFUL_TASKS_TEST_DATABASE_URL";

/// Environment variable that holds the URL of the dedicated sibling database.
pub const SIBLING_DATABASE_URL_ENV: &str = "CLOUDIFUL_TASKS_TEST_SIBLING_DATABASE_URL";

/// Database the live tests are allowed to touch.
pub const EXPECTED_DATABASE: &str = "cloudiful_tasks_test";

/// Sibling database of the fixture that holds a schema of the library too.
pub const SIBLING_DATABASE: &str = "cloudiful_tasks_dev";

/// Fixed configured endpoint of the shared non-production test server.
pub const EXPECTED_HOST: &str = "unraid.cloud1ful.com";

/// Fixed configured port of the shared non-production test server.
pub const EXPECTED_PORT: u16 = 5432;

/// Port reported by the test server backend.
///
/// The forwarded endpoint is not assumed to equal this server side value.
pub const EXPECTED_SERVER_PORT: &str = "5432";

/// Exact database comment of the dedicated test fixture.
pub const TEST_DATABASE_MARKER: &str = "cloudiful-tasks test fixture v1; non-production; dedicated";

/// Exact database comment of the dedicated sibling fixture.
pub const SIBLING_DATABASE_MARKER: &str =
    "cloudiful-tasks development fixture v1; non-production; dedicated";

const CLEANUP_SQL: &str = include_str!("../../sql/tests/migration_cleanup.sql");
const FOREIGN_SCHEMA_SQL: &str = include_str!("../../sql/tests/migration_foreign_schema.sql");
const EMPTY_SCHEMA_SQL: &str = include_str!("../../sql/tests/migration_empty_schema.sql");
const LEDGER_ONLY_SCHEMA_SQL: &str =
    include_str!("../../sql/tests/migration_ledger_only_schema.sql");
const PUBLIC_SENTINEL_SQL: &str = include_str!("../../sql/tests/migration_public_sentinel.sql");
const SEED_SENTINEL_SQL: &str = include_str!("../../sql/tests/migration_seed_sentinel.sql");
const ADVISORY_LOCKS_SQL: &str = include_str!("../../sql/tests/migration_advisory_locks.sql");
const INSERT_TASK_SQL: &str = include_str!("../../sql/tests/worker_insert_task.sql");

/// The statements that create the domain state of the application.
///
/// The schema is dropped first, so every test starts from an empty domain
/// state.
const APP_DOMAIN_SETUP_SQL: &str = include_str!("../../sql/tests/transaction_app_domain_setup.sql");

#[derive(sqlx::FromRow)]
struct IdentityRow {
    database_name: Option<String>,
    user_name: Option<String>,
    server_address: Option<String>,
    server_port: Option<String>,
    database_comment: Option<String>,
}

#[derive(sqlx::FromRow)]
struct RelationRow {
    relation_name: String,
}

fn fixture_options(env: &str, expected_database: &str) -> PgConnectOptions {
    let value = std::env::var(env).ok();
    fixture_options_from(value.as_deref(), expected_database)
        .unwrap_or_else(|reason| panic!("{env} {reason}"))
}

fn fixture_options_from(
    value: Option<&str>,
    expected_database: &str,
) -> std::result::Result<PgConnectOptions, String> {
    let url = value.ok_or_else(|| "must be set".to_owned())?;
    let options =
        PgConnectOptions::from_str(url).map_err(|_| "must be a valid postgres url".to_owned())?;
    validate_fixture_options(&options, expected_database)?;
    Ok(options)
}

fn validate_fixture_options(
    options: &PgConnectOptions,
    expected_database: &str,
) -> std::result::Result<(), String> {
    if options.get_database() != Some(expected_database) {
        return Err(format!(
            "must name the dedicated {expected_database} database"
        ));
    }
    if options.get_host() != EXPECTED_HOST {
        return Err(format!("must use the dedicated endpoint {EXPECTED_HOST}"));
    }
    if options.get_port() != EXPECTED_PORT {
        return Err(format!(
            "must use the dedicated endpoint port {EXPECTED_PORT}"
        ));
    }
    if options.get_username().is_empty() {
        return Err("must name a role".to_owned());
    }
    if options.get_socket().is_some() {
        return Err("must not use a Unix socket override".to_owned());
    }
    if options.get_options().is_some() {
        return Err("must not use connection option overrides".to_owned());
    }
    Ok(())
}

fn validate_identity(
    row: &IdentityRow,
    options: &PgConnectOptions,
    expected_database: &str,
    expected_marker: &str,
) -> std::result::Result<(), String> {
    if row.database_name.as_deref() != Some(expected_database) {
        return Err(format!(
            "must report the dedicated {expected_database} database, not {:?}",
            row.database_name
        ));
    }
    if row.user_name.as_deref() != Some(options.get_username()) {
        return Err(format!(
            "must report the configured role {}, not {:?}",
            options.get_username(),
            row.user_name
        ));
    }
    if row.database_comment.as_deref() != Some(expected_marker) {
        return Err(format!(
            "must carry the exact dedicated database marker, found {:?}",
            row.database_comment
        ));
    }
    match row.server_address.as_deref() {
        None => return Err("must be reached over TCP".to_owned()),
        Some("127.0.0.1" | "::1") => {
            return Err("must not be reached over a loopback address".to_owned());
        }
        Some(_) => {}
    }
    if row.server_port.as_deref() != Some(EXPECTED_SERVER_PORT) {
        return Err(format!(
            "must report server port {EXPECTED_SERVER_PORT}, not {:?}",
            row.server_port
        ));
    }
    Ok(())
}

/// The connection options of the dedicated test database.
pub fn test_database_options() -> PgConnectOptions {
    fixture_options(TEST_DATABASE_URL_ENV, EXPECTED_DATABASE)
}

/// Connects to the test database and verifies that it is the expected fixture.
pub async fn connect() -> PgPool {
    let options = test_database_options();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options.clone())
        .await
        .expect("the dedicated test database must be reachable");
    verify_identity(&pool, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).await;
    pool
}

/// Removes the schema of the library and the task owned fixtures.
pub async fn cleanup(pool: &PgPool) {
    sqlx::raw_sql(CLEANUP_SQL)
        .execute(pool)
        .await
        .expect("the test fixtures must be removable");
}

/// Creates a schema that the library does not own.
pub async fn create_foreign_schema(pool: &PgPool) {
    sqlx::raw_sql(FOREIGN_SCHEMA_SQL)
        .execute(pool)
        .await
        .expect("the foreign schema fixture must be creatable");
}

/// Creates an empty schema that the library does not own.
pub async fn create_empty_schema(pool: &PgPool) {
    sqlx::raw_sql(EMPTY_SCHEMA_SQL)
        .execute(pool)
        .await
        .expect("the empty schema fixture must be creatable");
}

/// Creates a schema that holds the migration ledger of the library alone.
pub async fn create_ledger_only_schema(pool: &PgPool) {
    sqlx::raw_sql(LEDGER_ONLY_SCHEMA_SQL)
        .execute(pool)
        .await
        .expect("the ledger only schema fixture must be creatable");
}

/// Creates the task owned tables in the public schema.
pub async fn create_public_sentinels(pool: &PgPool) {
    sqlx::raw_sql(PUBLIC_SENTINEL_SQL)
        .execute(pool)
        .await
        .expect("the public sentinel fixtures must be creatable");
    sqlx::raw_sql(SEED_SENTINEL_SQL)
        .execute(pool)
        .await
        .expect("the public sentinel fixture must be seedable");
}

/// The relations of a schema.
pub async fn relation_names(pool: &PgPool, schema: &str) -> Vec<String> {
    let rows = sqlx::query_file_as!(RelationRow, "sql/migration_schema_relations.sql", schema)
        .fetch_all(pool)
        .await
        .expect("the relations of the schema must be readable");
    rows.into_iter().map(|row| row.relation_name).collect()
}

/// Whether a schema exists.
pub async fn schema_exists(pool: &PgPool, schema: &str) -> bool {
    let row = sqlx::query_file!("sql/migration_schema_exists.sql", schema)
        .fetch_one(pool)
        .await
        .expect("the schema catalog must be readable");
    row.schema_exists.unwrap_or(false)
}

/// The number of advisory locks that are held in the database.
pub async fn advisory_lock_count(pool: &PgPool) -> i64 {
    let row = sqlx::raw_sql(ADVISORY_LOCKS_SQL)
        .fetch_one(pool)
        .await
        .expect("the lock catalog must be readable");
    row.try_get::<i64, _>("lock_count")
        .expect("the lock count must be a number")
}

/// Drops the schema of the library and creates it again.
pub async fn reset(pool: &PgPool) {
    cleanup(pool).await;
    cloudiful_tasks::migrate(pool)
        .await
        .expect("the schema of the library must be migratable");
}

/// A configuration with a short lease, so that leases expire while a test
/// runs.
pub fn short_lease_config() -> WorkerConfig {
    WorkerConfig {
        concurrency: 1,
        lease_ttl: Duration::from_millis(150),
        heartbeat_interval: Duration::from_millis(20),
        heartbeat_failure_budget: 3,
        cancel_poll_interval: Duration::from_millis(50),
        recovery_interval: Duration::from_millis(50),
        idle_poll_interval: Duration::from_millis(100),
        shutdown_grace: Duration::from_millis(100),
    }
}

/// A configuration with a lease that outlives a test.
pub fn test_config() -> WorkerConfig {
    WorkerConfig {
        concurrency: 1,
        lease_ttl: Duration::from_secs(30),
        heartbeat_interval: Duration::from_secs(5),
        heartbeat_failure_budget: 3,
        cancel_poll_interval: Duration::from_secs(3),
        recovery_interval: Duration::from_secs(5),
        idle_poll_interval: Duration::from_secs(10),
        shutdown_grace: Duration::from_secs(10),
    }
}

/// Opens a queue over the test database.
pub async fn open_queue(pool: &PgPool, namespace: &str, queue: &str) -> Queue {
    Queue::open(pool.clone(), namespace, queue, &test_config())
        .await
        .expect("the queue must be openable")
}

/// Opens a queue whose leases expire quickly.
pub async fn open_queue_with_short_lease(pool: &PgPool, namespace: &str, queue: &str) -> Queue {
    Queue::open(pool.clone(), namespace, queue, &short_lease_config())
        .await
        .expect("the queue must be openable")
}

/// Opens a queue with the configuration of a worker.
pub async fn open_queue_with_config(
    pool: &PgPool,
    namespace: &str,
    queue: &str,
    config: &WorkerConfig,
) -> Queue {
    Queue::open(pool.clone(), namespace, queue, config)
        .await
        .expect("the queue must be openable")
}

/// Connects to the sibling database of the fixture.
///
/// The sibling database holds a schema of the library too, so it proves that a
/// transaction of another database is refused. The tests only read from it and
/// roll their transactions back.
pub async fn connect_to_sibling_database() -> PgPool {
    let options = fixture_options(SIBLING_DATABASE_URL_ENV, SIBLING_DATABASE);
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .expect("the dedicated sibling database must be reachable");
    verify_identity(&pool, &options, SIBLING_DATABASE, SIBLING_DATABASE_MARKER).await;
    pool
}

/// Inserts a task without the wake up notification of the queue.
///
/// The worker must find the task with its polling, not with the notification.
pub async fn insert_task_without_notification(pool: &PgPool) {
    sqlx::raw_sql(INSERT_TASK_SQL)
        .execute(pool)
        .await
        .expect("the task must be insertable");
}

/// The backend that listens for the wake up channel.
pub async fn listening_backend(pool: &PgPool) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let row = sqlx::query_file_scalar!("sql/tests/worker_listening_backend.sql")
            .fetch_optional(pool)
            .await
            .expect("the sessions of the database must be readable");
        if let Some(Some(pid)) = row {
            return pid;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the listener of the worker did not connect in time"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Breaks the session of a backend of the test database.
pub async fn terminate_backend(pool: &PgPool, pid: i32) {
    let terminated = sqlx::query_file_scalar!("sql/tests/worker_terminate_backend.sql", pid)
        .fetch_one(pool)
        .await
        .expect("the session must be breakable")
        .unwrap_or(false);
    assert!(terminated, "the session must be broken");
}

/// The number of tasks of a namespace and queue.
pub async fn task_count(pool: &PgPool, namespace: &str, queue: &str) -> i64 {
    sqlx::query_file_scalar!("sql/tests/store_task_count.sql", namespace, queue)
        .fetch_one(pool)
        .await
        .expect("the tasks must be countable")
        .unwrap_or_default()
}

/// The number of attempts of a task.
pub async fn attempt_count(pool: &PgPool, task_id: uuid::Uuid) -> i64 {
    sqlx::query_file_scalar!("sql/tests/store_attempt_count.sql", task_id)
        .fetch_one(pool)
        .await
        .expect("the attempts must be countable")
        .unwrap_or_default()
}

/// Creates the domain state of the application and empties it.
///
/// The domain state belongs to the application, not to the library. The
/// library never writes to it, the tests write to it inside the transaction
/// of the caller to prove that a domain write and a queue change share one
/// commit.
pub async fn prepare_app_domain(pool: &PgPool) {
    sqlx::raw_sql(APP_DOMAIN_SETUP_SQL)
        .execute(pool)
        .await
        .expect("the domain state of the application must be creatable");
}

/// Writes a row of the domain state of the application.
///
/// The write runs in the transaction of the caller, so it becomes durable
/// together with the queue change of that transaction.
pub async fn write_app_domain(transaction: &mut Transaction<'_, Postgres>, note: &str) {
    sqlx::query_file!("sql/tests/transaction_app_domain_insert.sql", note)
        .execute(&mut **transaction)
        .await
        .expect("the domain state of the application must be writable");
}

/// The number of rows of the domain state of the application.
pub async fn app_domain_count(pool: &PgPool) -> i64 {
    sqlx::query_file_scalar!("sql/tests/transaction_app_domain_count.sql")
        .fetch_one(pool)
        .await
        .expect("the domain state of the application must be countable")
        .unwrap_or_default()
}

async fn verify_identity(
    pool: &PgPool,
    options: &PgConnectOptions,
    expected_database: &str,
    expected_marker: &str,
) {
    let row = sqlx::query_file_as!(IdentityRow, "sql/tests/migration_test_identity.sql")
        .fetch_one(pool)
        .await
        .expect("the dedicated database must report its identity");
    if let Err(reason) = validate_identity(&row, options, expected_database, expected_marker) {
        panic!("the {expected_database} fixture is invalid: {reason}");
    }
}

#[cfg(test)]
mod fixture_guard_tests {
    use super::*;

    fn options(url: &str) -> PgConnectOptions {
        PgConnectOptions::from_str(url).expect("the test fixture url must parse")
    }

    fn dedicated_options() -> PgConnectOptions {
        options("postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test")
    }

    fn identity() -> IdentityRow {
        IdentityRow {
            database_name: Some(EXPECTED_DATABASE.to_owned()),
            user_name: Some("postgres".to_owned()),
            server_address: Some("172.19.0.30".to_owned()),
            server_port: Some(EXPECTED_SERVER_PORT.to_owned()),
            database_comment: Some(TEST_DATABASE_MARKER.to_owned()),
        }
    }

    #[test]
    fn missing_malformed_and_mismatched_urls_fail_closed() {
        assert!(fixture_options_from(None, EXPECTED_DATABASE).is_err());
        assert!(fixture_options_from(Some("not a url"), EXPECTED_DATABASE).is_err());
        assert!(
            fixture_options_from(
                Some("postgres://postgres@unraid.cloud1ful.com:5432/postgres"),
                EXPECTED_DATABASE
            )
            .is_err()
        );
        assert!(
            fixture_options_from(
                Some("postgres://postgres@localhost:5432/cloudiful_tasks_test"),
                EXPECTED_DATABASE
            )
            .is_err()
        );
        assert!(
            fixture_options_from(
                Some("postgres://postgres@unraid.cloud1ful.com:5433/cloudiful_tasks_test"),
                EXPECTED_DATABASE
            )
            .is_err()
        );
        assert!(
            fixture_options_from(
                Some("postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test"),
                EXPECTED_DATABASE
            )
            .is_ok()
        );
    }

    #[test]
    fn connection_option_overrides_are_refused() {
        assert!(
            validate_fixture_options(
                &options(
                    "postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test?host=/tmp"
                ),
                EXPECTED_DATABASE
            )
            .is_err()
        );
        assert!(
            validate_fixture_options(
                &options("postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test?options=-csearch_path%3Dpublic"),
                EXPECTED_DATABASE
            )
            .is_err()
        );
    }

    #[test]
    fn identity_requires_the_dedicated_marker_and_endpoint() {
        let options = dedicated_options();
        let row = identity();
        assert!(validate_identity(&row, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_ok());

        let mut wrong = identity();
        wrong.database_name = Some("postgres".to_owned());
        assert!(
            validate_identity(&wrong, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_err()
        );

        let mut wrong = identity();
        wrong.database_comment = Some("another fixture".to_owned());
        assert!(
            validate_identity(&wrong, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_err()
        );

        let mut wrong = identity();
        wrong.user_name = Some("someone-else".to_owned());
        assert!(
            validate_identity(&wrong, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_err()
        );

        let mut wrong = identity();
        wrong.server_address = None;
        assert!(
            validate_identity(&wrong, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_err()
        );

        let mut wrong = identity();
        wrong.server_address = Some("127.0.0.1".to_owned());
        assert!(
            validate_identity(&wrong, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_err()
        );

        let mut wrong = identity();
        wrong.server_port = Some("5434".to_owned());
        assert!(
            validate_identity(&wrong, &options, EXPECTED_DATABASE, TEST_DATABASE_MARKER).is_err()
        );
    }
}
