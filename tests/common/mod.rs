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
use sea_orm::sqlx::postgres::PgConnectOptions;
use sea_orm::{
    ConnectOptions, ConnectionTrait, Database, DatabaseConnection, DatabaseTransaction, DbBackend,
    Statement, TryGetable,
};

/// Environment variable that holds the URL of the dedicated test database.
pub const TEST_DATABASE_URL_ENV: &str = "TASKS_TEST_DATABASE_URL";

/// Environment variable that holds the URL of the dedicated sibling database.
pub const SIBLING_DATABASE_URL_ENV: &str = "TASKS_TEST_SIBLING_DATABASE_URL";

/// Optional expectation override of the configured endpoint host.
pub const EXPECTED_HOST_ENV: &str = "TASKS_TEST_EXPECTED_HOST";

/// Optional expectation override of the configured endpoint port.
pub const EXPECTED_PORT_ENV: &str = "TASKS_TEST_EXPECTED_PORT";

/// Optional expectation override of the server reported port.
pub const EXPECTED_SERVER_PORT_ENV: &str = "TASKS_TEST_EXPECTED_SERVER_PORT";

/// Optional expectation override of the database the live tests touch.
pub const EXPECTED_DATABASE_ENV: &str = "TASKS_TEST_EXPECTED_DATABASE";

/// Optional expectation override of the sibling database.
pub const EXPECTED_SIBLING_DATABASE_ENV: &str = "TASKS_TEST_EXPECTED_SIBLING_DATABASE";

/// Exact database comment of the dedicated test fixture.
pub const TEST_DATABASE_MARKER: &str = "cloudiful-tasks test fixture v1; non-production; dedicated";

/// Exact database comment of the dedicated sibling fixture.
pub const SIBLING_DATABASE_MARKER: &str =
    "cloudiful-tasks development fixture v1; non-production; dedicated";

/// The expected identity of the dedicated fixture databases.
///
/// The legacy profile names the shared non-production fixture. An operator
/// that runs the live tests against an equivalent dedicated fixture supplies
/// the expectation overrides without changing the shipped defaults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetProfile {
    host: String,
    port: u16,
    server_port: String,
    database: String,
    sibling_database: String,
    database_marker: String,
    sibling_marker: String,
}

impl TargetProfile {
    /// The shared non-production fixture of the tasks checkout.
    pub fn legacy() -> Self {
        Self {
            host: "unraid.cloud1ful.com".to_owned(),
            port: 5432,
            server_port: "5432".to_owned(),
            database: "cloudiful_tasks_test".to_owned(),
            sibling_database: "cloudiful_tasks_dev".to_owned(),
            database_marker: TEST_DATABASE_MARKER.to_owned(),
            sibling_marker: SIBLING_DATABASE_MARKER.to_owned(),
        }
    }

    /// Builds a profile from explicit expectation inputs.
    ///
    /// The markers stay the exact non-production fixture markers, so an
    /// equivalent fixture is accepted only with the same database comments.
    pub fn from_inputs(
        host: &str,
        port: u16,
        server_port: &str,
        database: &str,
        sibling_database: &str,
    ) -> std::result::Result<Self, String> {
        if host.is_empty() {
            return Err("must name an endpoint host".to_owned());
        }
        if port == 0 {
            return Err("must name a non-zero endpoint port".to_owned());
        }
        if server_port
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .is_none()
        {
            return Err("must name a non-zero server port".to_owned());
        }
        if database.is_empty() {
            return Err("must name a test database".to_owned());
        }
        if sibling_database.is_empty() {
            return Err("must name a sibling database".to_owned());
        }
        if database == sibling_database {
            return Err("must name distinct test and sibling databases".to_owned());
        }
        Ok(Self {
            host: host.to_owned(),
            port,
            server_port: server_port.to_owned(),
            database: database.to_owned(),
            sibling_database: sibling_database.to_owned(),
            database_marker: TEST_DATABASE_MARKER.to_owned(),
            sibling_marker: SIBLING_DATABASE_MARKER.to_owned(),
        })
    }

    /// Reads the expectation overrides and falls back to the legacy profile.
    fn from_env() -> std::result::Result<Self, String> {
        let legacy = Self::legacy();
        let host = std::env::var(EXPECTED_HOST_ENV).unwrap_or(legacy.host);
        let port = match std::env::var(EXPECTED_PORT_ENV) {
            Ok(value) => value
                .parse::<u16>()
                .map_err(|_| format!("{EXPECTED_PORT_ENV} must be a port number"))?,
            Err(_) => legacy.port,
        };
        let server_port = std::env::var(EXPECTED_SERVER_PORT_ENV).unwrap_or(legacy.server_port);
        let database = std::env::var(EXPECTED_DATABASE_ENV).unwrap_or(legacy.database);
        let sibling_database =
            std::env::var(EXPECTED_SIBLING_DATABASE_ENV).unwrap_or(legacy.sibling_database);
        Self::from_inputs(&host, port, &server_port, &database, &sibling_database)
    }

    /// The test database of the profile.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The sibling database of the profile.
    pub fn sibling_database(&self) -> &str {
        &self.sibling_database
    }

    fn database_marker(&self, database: &str) -> &str {
        if database == self.database {
            &self.database_marker
        } else {
            &self.sibling_marker
        }
    }
}

fn target_profile() -> TargetProfile {
    TargetProfile::from_env()
        .unwrap_or_else(|reason| panic!("the expected fixture is invalid: {reason}"))
}

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

struct IdentityRow {
    database_name: Option<String>,
    user_name: Option<String>,
    server_address: Option<String>,
    server_port: Option<String>,
    database_comment: Option<String>,
}

fn fixture_options(
    env: &str,
    expected_database: &str,
    profile: &TargetProfile,
) -> PgConnectOptions {
    let value = std::env::var(env).ok();
    fixture_options_from(value.as_deref(), expected_database, profile)
        .unwrap_or_else(|reason| panic!("{env} {reason}"))
}

fn fixture_options_from(
    value: Option<&str>,
    expected_database: &str,
    profile: &TargetProfile,
) -> std::result::Result<PgConnectOptions, String> {
    let url = value.ok_or_else(|| "must be set".to_owned())?;
    let options =
        PgConnectOptions::from_str(url).map_err(|_| "must be a valid postgres url".to_owned())?;
    validate_fixture_options(&options, expected_database, profile)?;
    Ok(options)
}

fn validate_fixture_options(
    options: &PgConnectOptions,
    expected_database: &str,
    profile: &TargetProfile,
) -> std::result::Result<(), String> {
    if options.get_database() != Some(expected_database) {
        return Err(format!(
            "must name the dedicated {expected_database} database"
        ));
    }
    if options.get_host() != profile.host.as_str() {
        return Err(format!("must use the dedicated endpoint {}", profile.host));
    }
    if options.get_port() != profile.port {
        return Err(format!(
            "must use the dedicated endpoint port {}",
            profile.port
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
    profile: &TargetProfile,
    expected_database: &str,
) -> std::result::Result<(), String> {
    let expected_marker = profile.database_marker(expected_database);
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
    if row.server_port.as_deref() != Some(profile.server_port.as_str()) {
        return Err(format!(
            "must report server port {}, not {:?}",
            profile.server_port, row.server_port
        ));
    }
    Ok(())
}

/// A SQL statement with positional values.
pub fn statement(sql: &str, values: Vec<sea_orm::Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Postgres, sql, values)
}

/// A single scalar value from a statement.
pub async fn fetch_scalar<T: TryGetable>(
    db: &DatabaseConnection,
    statement: Statement,
) -> Option<T> {
    db.query_one_raw(statement)
        .await
        .expect("the scalar statement must run")
        .map(|row| {
            row.try_get_by::<T, usize>(0)
                .expect("the scalar value must decode")
        })
}

/// Connects to the test database with a fresh pool and verifies its identity.
pub async fn connect_worker() -> DatabaseConnection {
    connect_worker_with(4).await
}

/// Connects to the test database with a fresh pool of the given size.
pub async fn connect_worker_with(max_connections: u32) -> DatabaseConnection {
    let profile = target_profile();
    let options = fixture_options(TEST_DATABASE_URL_ENV, &profile.database, &profile);
    let url = std::env::var(TEST_DATABASE_URL_ENV).expect("the test database url must be set");
    let mut connect = ConnectOptions::new(url);
    connect.max_connections(max_connections);
    let db = Database::connect(connect)
        .await
        .expect("the dedicated worker database must be reachable");
    verify_identity(&db, &options, &profile, &profile.database).await;
    db
}

/// Connects to the test database and verifies that it is the expected fixture.
pub async fn connect() -> DatabaseConnection {
    connect_worker().await
}

/// Removes the schema of the library and the task owned fixtures.
pub async fn cleanup(db: &DatabaseConnection) {
    db.execute_unprepared(CLEANUP_SQL)
        .await
        .expect("the test fixtures must be removable");
}

/// Creates a schema that the library does not own.
pub async fn create_foreign_schema(db: &DatabaseConnection) {
    db.execute_unprepared(FOREIGN_SCHEMA_SQL)
        .await
        .expect("the foreign schema fixture must be creatable");
}

/// Creates an empty schema that the library does not own.
pub async fn create_empty_schema(db: &DatabaseConnection) {
    db.execute_unprepared(EMPTY_SCHEMA_SQL)
        .await
        .expect("the empty schema fixture must be creatable");
}

/// Creates a schema that holds the migration ledger of the library alone.
pub async fn create_ledger_only_schema(db: &DatabaseConnection) {
    db.execute_unprepared(LEDGER_ONLY_SCHEMA_SQL)
        .await
        .expect("the ledger only schema fixture must be creatable");
}

/// Creates the task owned tables in the public schema.
pub async fn create_public_sentinels(db: &DatabaseConnection) {
    db.execute_unprepared(PUBLIC_SENTINEL_SQL)
        .await
        .expect("the public sentinel fixtures must be creatable");
    db.execute_unprepared(SEED_SENTINEL_SQL)
        .await
        .expect("the public sentinel fixture must be seedable");
}

/// The relations of a schema.
pub async fn relation_names(db: &DatabaseConnection, schema: &str) -> Vec<String> {
    let rows = db
        .query_all_raw(statement(
            include_str!("../../sql/migration_schema_relations.sql"),
            vec![schema.into()],
        ))
        .await
        .expect("the relations of the schema must be readable");
    rows.into_iter()
        .map(|row| {
            row.try_get_by::<String, usize>(0)
                .expect("the relation name must decode")
        })
        .collect()
}

/// Whether a schema exists.
pub async fn schema_exists(db: &DatabaseConnection, schema: &str) -> bool {
    fetch_scalar::<bool>(
        db,
        statement(
            include_str!("../../sql/migration_schema_exists.sql"),
            vec![schema.into()],
        ),
    )
    .await
    .unwrap_or(false)
}

/// The number of advisory locks that are held in the database.
pub async fn advisory_lock_count(db: &DatabaseConnection) -> i64 {
    fetch_scalar::<i64>(db, statement(ADVISORY_LOCKS_SQL, Vec::new()))
        .await
        .expect("the lock count must be a number")
}

/// Drops the schema of the library and creates it again.
pub async fn reset(db: &DatabaseConnection) {
    cleanup(db).await;
    cloudiful_tasks::migrate(db)
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
pub async fn open_queue(db: &DatabaseConnection, namespace: &str, queue: &str) -> Queue {
    Queue::open(db.clone(), namespace, queue, &test_config())
        .await
        .expect("the queue must be openable")
}

/// Opens a queue whose leases expire quickly.
pub async fn open_queue_with_short_lease(
    db: &DatabaseConnection,
    namespace: &str,
    queue: &str,
) -> Queue {
    Queue::open(db.clone(), namespace, queue, &short_lease_config())
        .await
        .expect("the queue must be openable")
}

/// Opens a queue with the configuration of a worker.
pub async fn open_queue_with_config(
    db: &DatabaseConnection,
    namespace: &str,
    queue: &str,
    config: &WorkerConfig,
) -> Queue {
    Queue::open(db.clone(), namespace, queue, config)
        .await
        .expect("the queue must be openable")
}

/// Connects to the sibling database of the fixture.
///
/// The sibling database holds a schema of the library too, so it proves that a
/// transaction of another database is refused. The tests only read from it and
/// roll their transactions back.
pub async fn connect_to_sibling_database() -> DatabaseConnection {
    let profile = target_profile();
    let options = fixture_options(
        SIBLING_DATABASE_URL_ENV,
        &profile.sibling_database,
        &profile,
    );
    let url =
        std::env::var(SIBLING_DATABASE_URL_ENV).expect("the sibling database url must be set");
    let mut connect = ConnectOptions::new(url);
    connect.max_connections(2);
    let db = Database::connect(connect)
        .await
        .expect("the dedicated sibling database must be reachable");
    verify_identity(&db, &options, &profile, &profile.sibling_database).await;
    db
}

/// Inserts a task without the wake up notification of the queue.
///
/// The worker must find the task with its polling, not with the notification.
pub async fn insert_task_without_notification(db: &DatabaseConnection) {
    db.execute_unprepared(INSERT_TASK_SQL)
        .await
        .expect("the task must be insertable");
}

/// The backend that listens for the wake up channel.
pub async fn listening_backend(db: &DatabaseConnection) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let value = fetch_scalar::<Option<i32>>(
            db,
            statement(
                include_str!("../../sql/tests/worker_listening_backend.sql"),
                Vec::new(),
            ),
        )
        .await
        .flatten();
        if let Some(pid) = value {
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
pub async fn terminate_backend(db: &DatabaseConnection, pid: i32) {
    let terminated = fetch_scalar::<Option<bool>>(
        db,
        statement(
            include_str!("../../sql/tests/worker_terminate_backend.sql"),
            vec![pid.into()],
        ),
    )
    .await
    .flatten();
    assert_eq!(terminated, Some(true), "the session must be broken");
}

/// The number of tasks of a namespace and queue.
pub async fn task_count(db: &DatabaseConnection, namespace: &str, queue: &str) -> i64 {
    fetch_scalar::<i64>(
        db,
        statement(
            include_str!("../../sql/tests/store_task_count.sql"),
            vec![namespace.into(), queue.into()],
        ),
    )
    .await
    .unwrap_or_default()
}

/// The number of attempts of a task.
pub async fn attempt_count(db: &DatabaseConnection, task_id: uuid::Uuid) -> i64 {
    fetch_scalar::<i64>(
        db,
        statement(
            include_str!("../../sql/tests/store_attempt_count.sql"),
            vec![task_id.into()],
        ),
    )
    .await
    .unwrap_or_default()
}

/// Creates the domain state of the application and empties it.
///
/// The domain state belongs to the application, not to the library. The
/// library never writes to it, the tests write to it inside the transaction
/// of the caller to prove that a domain write and a queue change share one
/// commit.
pub async fn prepare_app_domain(db: &DatabaseConnection) {
    db.execute_unprepared(APP_DOMAIN_SETUP_SQL)
        .await
        .expect("the domain state of the application must be creatable");
}

/// Writes a row of the domain state of the application.
///
/// The write runs in the transaction of the caller, so it becomes durable
/// together with the queue change of that transaction.
pub async fn write_app_domain(transaction: &mut DatabaseTransaction, note: &str) {
    transaction
        .execute_raw(statement(
            include_str!("../../sql/tests/transaction_app_domain_insert.sql"),
            vec![note.into()],
        ))
        .await
        .expect("the domain state of the application must be writable");
}

/// The number of rows of the domain state of the application.
pub async fn app_domain_count(db: &DatabaseConnection) -> i64 {
    fetch_scalar::<i64>(
        db,
        statement(
            include_str!("../../sql/tests/transaction_app_domain_count.sql"),
            Vec::new(),
        ),
    )
    .await
    .unwrap_or_default()
}

async fn verify_identity(
    db: &DatabaseConnection,
    options: &PgConnectOptions,
    profile: &TargetProfile,
    expected_database: &str,
) {
    let row = db
        .query_one_raw(statement(
            include_str!("../../sql/tests/migration_test_identity.sql"),
            Vec::new(),
        ))
        .await
        .expect("the dedicated database must report its identity")
        .expect("the dedicated database must report its identity");
    let row = IdentityRow {
        database_name: row.try_get_by::<Option<String>, usize>(0).unwrap(),
        user_name: row.try_get_by::<Option<String>, usize>(1).unwrap(),
        server_address: row.try_get_by::<Option<String>, usize>(2).unwrap(),
        server_port: row.try_get_by::<Option<String>, usize>(3).unwrap(),
        database_comment: row.try_get_by::<Option<String>, usize>(4).unwrap(),
    };
    if let Err(reason) = validate_identity(&row, options, profile, expected_database) {
        panic!("the {expected_database} fixture is invalid: {reason}");
    }
}

#[cfg(test)]
mod fixture_guard_tests {
    use super::*;

    fn options(url: &str) -> PgConnectOptions {
        PgConnectOptions::from_str(url).expect("the test fixture url must parse")
    }

    fn legacy_profile() -> TargetProfile {
        TargetProfile::legacy()
    }

    fn dedicated_options(profile: &TargetProfile) -> PgConnectOptions {
        options(&format!(
            "postgres://postgres@{}:{}/{}",
            profile.host, profile.port, profile.database
        ))
    }

    fn identity(profile: &TargetProfile) -> IdentityRow {
        IdentityRow {
            database_name: Some(profile.database.clone()),
            user_name: Some("postgres".to_owned()),
            server_address: Some("172.19.0.30".to_owned()),
            server_port: Some(profile.server_port.clone()),
            database_comment: Some(profile.database_marker.clone()),
        }
    }

    #[test]
    fn missing_malformed_and_mismatched_urls_fail_closed() {
        let profile = legacy_profile();
        assert!(fixture_options_from(None, &profile.database, &profile).is_err());
        assert!(fixture_options_from(Some("not a url"), &profile.database, &profile).is_err());
        assert!(
            fixture_options_from(
                Some("postgres://postgres@unraid.cloud1ful.com:5432/postgres"),
                &profile.database,
                &profile
            )
            .is_err()
        );
        assert!(
            fixture_options_from(
                Some("postgres://postgres@localhost:5432/cloudiful_tasks_test"),
                &profile.database,
                &profile
            )
            .is_err()
        );
        assert!(
            fixture_options_from(
                Some("postgres://postgres@unraid.cloud1ful.com:5433/cloudiful_tasks_test"),
                &profile.database,
                &profile
            )
            .is_err()
        );
        assert!(
            fixture_options_from(
                Some("postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test"),
                &profile.database,
                &profile
            )
            .is_ok()
        );
    }

    #[test]
    fn connection_option_overrides_are_refused() {
        let profile = legacy_profile();
        assert!(
            validate_fixture_options(
                &options(
                    "postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test?host=/tmp"
                ),
                &profile.database,
                &profile
            )
            .is_err()
        );
        assert!(
            validate_fixture_options(
                &options("postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_test?options=-csearch_path%3Dpublic"),
                &profile.database,
                &profile
            )
            .is_err()
        );
    }

    #[test]
    fn identity_requires_the_dedicated_marker_and_endpoint() {
        let profile = legacy_profile();
        let options = dedicated_options(&profile);
        let row = identity(&profile);
        assert!(validate_identity(&row, &options, &profile, &profile.database).is_ok());

        let mut wrong = identity(&profile);
        wrong.database_name = Some("postgres".to_owned());
        assert!(validate_identity(&wrong, &options, &profile, &profile.database).is_err());

        let mut wrong = identity(&profile);
        wrong.database_comment = Some("another fixture".to_owned());
        assert!(validate_identity(&wrong, &options, &profile, &profile.database).is_err());

        let mut wrong = identity(&profile);
        wrong.user_name = Some("someone-else".to_owned());
        assert!(validate_identity(&wrong, &options, &profile, &profile.database).is_err());

        let mut wrong = identity(&profile);
        wrong.server_address = None;
        assert!(validate_identity(&wrong, &options, &profile, &profile.database).is_err());

        let mut wrong = identity(&profile);
        wrong.server_address = Some("127.0.0.1".to_owned());
        assert!(validate_identity(&wrong, &options, &profile, &profile.database).is_err());

        let mut wrong = identity(&profile);
        wrong.server_port = Some("5434".to_owned());
        assert!(validate_identity(&wrong, &options, &profile, &profile.database).is_err());
    }

    #[test]
    fn an_explicit_profile_is_validated_and_accepted() {
        let profile = TargetProfile::from_inputs(
            "unraid.cloud1ful.com",
            5439,
            "5432",
            "context69_issue_21_tasks_test",
            "context69_issue_21_tasks_dev",
        )
        .expect("the explicit profile must be valid");
        assert_eq!(profile.database(), "context69_issue_21_tasks_test");
        assert_eq!(profile.sibling_database(), "context69_issue_21_tasks_dev");
        assert_eq!(profile.port, 5439);

        let connect_options = dedicated_options(&profile);
        assert!(validate_fixture_options(&connect_options, profile.database(), &profile).is_ok());
        // The same endpoint with the legacy database name is refused.
        assert!(
            validate_fixture_options(&connect_options, "cloudiful_tasks_test", &profile).is_err()
        );

        let row = identity(&profile);
        assert!(validate_identity(&row, &connect_options, &profile, profile.database()).is_ok());

        // A connection to another port is refused by the profile.
        let foreign =
            options("postgres://postgres@unraid.cloud1ful.com:5432/context69_issue_21_tasks_test");
        assert!(validate_fixture_options(&foreign, profile.database(), &profile).is_err());

        assert!(TargetProfile::from_inputs("", 5439, "5432", "a", "b").is_err());
        assert!(TargetProfile::from_inputs("host", 0, "5432", "a", "b").is_err());
        assert!(TargetProfile::from_inputs("host", 5439, "0", "a", "b").is_err());
        assert!(TargetProfile::from_inputs("host", 5439, "5432", "a", "a").is_err());
    }
}
