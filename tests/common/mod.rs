//! Shared helpers for the live integration tests.
//!
//! The helpers fail the test when the test database is missing or does not
//! match the expected fixture identity. A live test never reports success
//! without a verified database.

// The helpers are shared by the test binaries, not every binary uses all of
// them.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use cloudiful_tasks::{Queue, WorkerConfig};
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};

/// Environment variable that holds the URL of the test database.
pub const TEST_DATABASE_URL_ENV: &str = "CLOUDIFUL_TASKS_TEST_DATABASE_URL";

/// Database the live tests are allowed to touch.
pub const EXPECTED_DATABASE: &str = "cloudiful_tasks_test";

/// Sibling database of the fixture that holds a schema of the library too.
pub const SIBLING_DATABASE: &str = "cloudiful_tasks_dev";

/// Role the live tests are allowed to use.
pub const EXPECTED_USER: &str = "dev";

/// Port the test database listens on.
pub const EXPECTED_PORT: u16 = 57664;

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
    socket_directories: Option<String>,
}

#[derive(sqlx::FromRow)]
struct RelationRow {
    relation_name: String,
}

/// The URL of the test database.
///
/// The test fails when the variable is missing: a live test never reports
/// success without a database.
pub fn test_database_url() -> String {
    std::env::var(TEST_DATABASE_URL_ENV).unwrap_or_else(|_| {
        panic!("{TEST_DATABASE_URL_ENV} must point at the {EXPECTED_DATABASE} database")
    })
}

/// The connection options of the test database.
pub fn test_database_options() -> PgConnectOptions {
    let url = test_database_url();
    let options = PgConnectOptions::from_str(&url).unwrap_or_else(|error| {
        panic!("the test database url must be a valid postgres url: {error}")
    });
    assert_eq!(
        options.get_database(),
        Some(EXPECTED_DATABASE),
        "the test database url must name the {EXPECTED_DATABASE} database"
    );
    options
}

/// Connects to the test database and verifies that it is the expected fixture.
pub async fn connect() -> PgPool {
    let url = test_database_url();
    let options = PgConnectOptions::from_str(&url).unwrap_or_else(|error| {
        panic!("the test database url must be a valid postgres url: {error}")
    });

    assert_eq!(
        options.get_database(),
        Some(EXPECTED_DATABASE),
        "the test database url must name the {EXPECTED_DATABASE} database"
    );
    assert_eq!(
        options.get_username(),
        EXPECTED_USER,
        "the test database url must use the {EXPECTED_USER} role"
    );
    assert_eq!(
        options.get_port(),
        EXPECTED_PORT,
        "the test database url must use port {EXPECTED_PORT}"
    );

    let socket = configured_socket_path(&url);
    assert_task_socket(&socket);

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .expect("the test database must be reachable");
    verify_identity(&pool, &socket).await;
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
    let url = test_database_url().replace(EXPECTED_DATABASE, SIBLING_DATABASE);
    let options = PgConnectOptions::from_str(&url).unwrap_or_else(|error| {
        panic!("the sibling database url must be a valid postgres url: {error}")
    });
    assert_eq!(
        options.get_database(),
        Some(SIBLING_DATABASE),
        "the sibling database url must name the {SIBLING_DATABASE} database"
    );
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options)
        .await
        .expect("the sibling database must be reachable")
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

fn configured_socket_path(url: &str) -> PathBuf {
    let query = url.split_once('?').map(|(_, query)| query).unwrap_or("");
    for pair in query.split('&') {
        if let Some(value) = pair.strip_prefix("host=") {
            return PathBuf::from(value);
        }
    }
    panic!("the test database url must point at the unix socket of the tasks checkout");
}

fn assert_task_socket(socket: &Path) {
    assert!(
        socket.is_absolute(),
        "the test database socket must be an absolute path: {socket:?}"
    );
    assert!(
        socket.ends_with("target/pg"),
        "the test database socket must live in the target directory of the tasks checkout: {socket:?}"
    );
    let root = socket
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| panic!("the socket path must live in <root>/target/pg: {socket:?}"));
    let manifest = root.join("Cargo.toml");
    assert!(
        manifest.is_file(),
        "the socket path must belong to a checkout of the library: {root:?}"
    );
    let manifest =
        std::fs::read_to_string(&manifest).expect("the checkout manifest must be readable");
    assert!(
        manifest.contains("name = \"cloudiful-tasks\""),
        "the socket path must belong to a checkout of the library: {root:?}"
    );
}

async fn verify_identity(pool: &PgPool, socket: &Path) {
    let row = sqlx::query_file_as!(IdentityRow, "sql/tests/migration_test_identity.sql")
        .fetch_one(pool)
        .await
        .expect("the test database must report its identity");

    assert_eq!(row.database_name.as_deref(), Some(EXPECTED_DATABASE));
    assert_eq!(row.user_name.as_deref(), Some(EXPECTED_USER));
    assert!(
        row.server_address.is_none(),
        "the test database must not listen on a network address"
    );
    assert_eq!(row.server_port.as_deref(), Some("57664"));

    let expected = std::fs::canonicalize(socket).unwrap_or_else(|_| socket.to_path_buf());
    assert!(
        row.socket_directories
            .as_deref()
            .unwrap_or_default()
            .split(':')
            .any(|directory| Path::new(directory) == expected),
        "the test database must listen on the socket of the tasks checkout: {:?}",
        row.socket_directories
    );
}
