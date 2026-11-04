//! Shared helpers for the live integration tests.
//!
//! The helpers fail the test when the issue database pair is missing,
//! mismatched or does not match its exact identity. A live test never reports
//! success without a verified issue-owned database.

// The helpers are shared by the test binaries, not every binary uses all of
// them.
#![allow(dead_code)]

use std::str::FromStr;
use std::time::Duration;

use cloudiful_tasks::{Queue, WorkerConfig};
use sqlx::postgres::PgConnectOptions;
use sqlx::{PgPool, Postgres, Row, Transaction};

/// Environment variable that holds the URL of the issue primary database.
pub const PRIMARY_DATABASE_URL_ENV: &str = "CLOUDIFUL_TASKS_TEST_DATABASE_URL";

/// Environment variable that holds the URL of the issue sibling database.
pub const SIBLING_DATABASE_URL_ENV: &str = "CLOUDIFUL_TASKS_TEST_SIBLING_DATABASE_URL";

/// Fixed configured endpoint of the non-production test server.
pub const EXPECTED_HOST: &str = "unraid.cloud1ful.com";

/// Fixed configured port of the non-production test server.
pub const EXPECTED_PORT: u16 = 5432;

/// Port reported by the test server backend.
///
/// The forwarded endpoint is not assumed to equal this server side value.
pub const EXPECTED_SERVER_PORT: &str = "5432";

const ISSUE_DATABASE_PREFIX: &str = "cloudiful_tasks_issue_";
const SIBLING_DATABASE_SUFFIX: &str = "_sibling";
const MAX_DATABASE_NAME_LENGTH: usize = 63;

/// The exact database names and public markers of one issue-owned fixture pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FixturePair {
    issue: u32,
    primary: String,
    sibling: String,
    primary_marker: String,
    sibling_marker: String,
}

impl FixturePair {
    /// Builds the fixture pair of a positive issue number.
    pub fn for_issue(issue: u32) -> std::result::Result<Self, String> {
        if issue == 0 {
            return Err("must name a positive issue id".to_owned());
        }
        let primary = format!("{ISSUE_DATABASE_PREFIX}{issue}");
        let sibling = format!("{primary}{SIBLING_DATABASE_SUFFIX}");
        if primary.len() > MAX_DATABASE_NAME_LENGTH || sibling.len() > MAX_DATABASE_NAME_LENGTH {
            return Err(format!(
                "must keep both issue database names within {MAX_DATABASE_NAME_LENGTH} bytes"
            ));
        }
        Ok(Self {
            issue,
            primary_marker: format!(
                "cloudiful-tasks issue {issue} test fixture v1; non-production; issue-owned"
            ),
            sibling_marker: format!(
                "cloudiful-tasks issue {issue} sibling fixture v1; non-production; issue-owned"
            ),
            primary,
            sibling,
        })
    }

    /// Parses the exact primary and sibling names of one issue pair.
    pub fn from_names(primary: &str, sibling: &str) -> std::result::Result<Self, String> {
        let Some(rest) = primary.strip_prefix(ISSUE_DATABASE_PREFIX) else {
            return Err(format!(
                "must name an issue primary database such as {ISSUE_DATABASE_PREFIX}<id>"
            ));
        };
        if rest.is_empty()
            || rest.starts_with('0')
            || !rest.chars().all(|character| character.is_ascii_digit())
        {
            return Err("must name a canonical positive issue id without leading zeros".to_owned());
        }
        let issue: u32 = rest
            .parse()
            .map_err(|_| "must name an issue id within the u32 range".to_owned())?;
        let pair = Self::for_issue(issue)?;
        if primary != pair.primary {
            return Err("must name the canonical primary database of the issue".to_owned());
        }
        if sibling != pair.sibling {
            return Err("must name the sibling database of the same issue pair".to_owned());
        }
        Ok(pair)
    }

    /// The issue that owns the pair.
    pub fn issue(&self) -> u32 {
        self.issue
    }

    /// The exact primary database name.
    pub fn primary(&self) -> &str {
        &self.primary
    }

    /// The exact sibling database name.
    pub fn sibling(&self) -> &str {
        &self.sibling
    }

    /// The exact database comment of the primary fixture.
    pub fn primary_marker(&self) -> &str {
        &self.primary_marker
    }

    /// The exact database comment of the sibling fixture.
    pub fn sibling_marker(&self) -> &str {
        &self.sibling_marker
    }
}

struct FixtureConfig {
    pair: FixturePair,
    primary: PgConnectOptions,
    sibling: PgConnectOptions,
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

fn parse_options(url: &str) -> std::result::Result<PgConnectOptions, String> {
    PgConnectOptions::from_str(url).map_err(|_| "must be a valid postgres url".to_owned())
}

fn fixture_config_from_values(
    primary: Option<&str>,
    sibling: Option<&str>,
) -> std::result::Result<FixtureConfig, String> {
    let primary_url = primary.ok_or_else(|| format!("{PRIMARY_DATABASE_URL_ENV} must be set"))?;
    let sibling_url = sibling.ok_or_else(|| format!("{SIBLING_DATABASE_URL_ENV} must be set"))?;
    let primary_options = parse_options(primary_url)?;
    let sibling_options = parse_options(sibling_url)?;
    let primary_name = primary_options
        .get_database()
        .ok_or_else(|| "must name the issue primary database".to_owned())?;
    let sibling_name = sibling_options
        .get_database()
        .ok_or_else(|| "must name the issue sibling database".to_owned())?;
    let pair = FixturePair::from_names(primary_name, sibling_name)?;
    validate_fixture_options(&primary_options, pair.primary())?;
    validate_fixture_options(&sibling_options, pair.sibling())?;
    Ok(FixtureConfig {
        pair,
        primary: primary_options,
        sibling: sibling_options,
    })
}

fn fixture_config() -> FixtureConfig {
    let primary = std::env::var(PRIMARY_DATABASE_URL_ENV).ok();
    let sibling = std::env::var(SIBLING_DATABASE_URL_ENV).ok();
    fixture_config_from_values(primary.as_deref(), sibling.as_deref())
        .unwrap_or_else(|reason| panic!("the issue fixture pair is invalid: {reason}"))
}

fn validate_fixture_options(
    options: &PgConnectOptions,
    expected_database: &str,
) -> std::result::Result<(), String> {
    if options.get_database() != Some(expected_database) {
        return Err(format!(
            "must name the issue-owned {expected_database} database"
        ));
    }
    if options.get_host() != EXPECTED_HOST {
        return Err(format!("must use the fixed endpoint {EXPECTED_HOST}"));
    }
    if options.get_port() != EXPECTED_PORT {
        return Err(format!("must use the fixed endpoint port {EXPECTED_PORT}"));
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
            "must report the issue-owned {expected_database} database, not {:?}",
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
            "must carry the exact issue-owned database marker, found {:?}",
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

/// The connection options of the issue primary database.
///
/// The complete issue pair is read and validated before the primary database
/// is used, so a missing or mismatched sibling value refuses the pair.
pub fn test_database_options() -> PgConnectOptions {
    fixture_config().primary
}

/// Connects to the issue primary database and verifies its identity.
pub async fn connect() -> PgPool {
    let config = fixture_config();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(config.primary.clone())
        .await
        .expect("the issue primary database must be reachable");
    verify_identity(
        &pool,
        &config.primary,
        config.pair.primary(),
        config.pair.primary_marker(),
    )
    .await;
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
    let FixtureConfig {
        pair,
        sibling: options,
        ..
    } = fixture_config();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(options.clone())
        .await
        .expect("the issue sibling database must be reachable");
    verify_identity(&pool, &options, pair.sibling(), pair.sibling_marker()).await;
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
        .expect("the issue-owned database must report its identity");
    if let Err(reason) = validate_identity(&row, options, expected_database, expected_marker) {
        panic!("the {expected_database} fixture is invalid: {reason}");
    }
}

#[cfg(test)]
mod fixture_guard_tests {
    use super::*;

    fn url(database: &str) -> String {
        format!("postgres://postgres@unraid.cloud1ful.com:5432/{database}")
    }

    fn pair() -> FixturePair {
        FixturePair::for_issue(1194).expect("the example issue pair must be valid")
    }

    fn identity(pair: &FixturePair) -> IdentityRow {
        IdentityRow {
            database_name: Some(pair.primary().to_owned()),
            user_name: Some("postgres".to_owned()),
            server_address: Some("172.19.0.30".to_owned()),
            server_port: Some(EXPECTED_SERVER_PORT.to_owned()),
            database_comment: Some(pair.primary_marker().to_owned()),
        }
    }

    #[test]
    fn canonical_pair_and_markers_are_exact() {
        let pair = pair();
        assert_eq!(pair.issue(), 1194);
        assert_eq!(pair.primary(), "cloudiful_tasks_issue_1194");
        assert_eq!(pair.sibling(), "cloudiful_tasks_issue_1194_sibling");
        assert_eq!(
            pair.primary_marker(),
            "cloudiful-tasks issue 1194 test fixture v1; non-production; issue-owned"
        );
        assert_eq!(
            pair.sibling_marker(),
            "cloudiful-tasks issue 1194 sibling fixture v1; non-production; issue-owned"
        );
        assert_eq!(
            FixturePair::from_names(pair.primary(), pair.sibling()).unwrap(),
            pair
        );

        let config =
            fixture_config_from_values(Some(&url(pair.primary())), Some(&url(pair.sibling())))
                .expect("the canonical issue pair must be accepted");
        assert_eq!(config.pair, pair);
    }

    #[test]
    fn non_canonical_issue_names_are_refused() {
        let sibling = pair().sibling().to_owned();
        for primary in [
            format!("{ISSUE_DATABASE_PREFIX}0"),
            format!("{ISSUE_DATABASE_PREFIX}01194"),
            format!("{ISSUE_DATABASE_PREFIX}1194x"),
            ISSUE_DATABASE_PREFIX.to_owned(),
        ] {
            assert!(
                FixturePair::from_names(&primary, &sibling).is_err(),
                "must refuse {primary}"
            );
        }
        assert!(FixturePair::for_issue(0).is_err());
    }

    #[test]
    fn legacy_shared_names_are_refused() {
        let shared_primary = url("cloudiful_tasks_test");
        let shared_sibling = url("cloudiful_tasks_dev");
        assert!(fixture_config_from_values(Some(&shared_primary), Some(&shared_sibling)).is_err());
        assert!(FixturePair::from_names("cloudiful_tasks_dev", "cloudiful_tasks_test").is_err());
    }

    #[test]
    fn mismatched_and_missing_pair_values_are_refused() {
        let pair = pair();
        let primary = url(pair.primary());
        let sibling = url(pair.sibling());
        assert!(fixture_config_from_values(None, Some(&sibling)).is_err());
        assert!(fixture_config_from_values(Some(&primary), None).is_err());
        assert!(
            fixture_config_from_values(
                Some(&primary),
                Some(&url("cloudiful_tasks_issue_1195_sibling"))
            )
            .is_err()
        );
        assert!(
            fixture_config_from_values(Some(&url("cloudiful_tasks_issue_1195")), Some(&sibling))
                .is_err()
        );
    }

    #[test]
    fn malformed_and_mismatched_endpoints_fail_closed() {
        let pair = pair();
        let sibling = url(pair.sibling());
        assert!(fixture_config_from_values(Some("not a url"), Some(&sibling)).is_err());
        for primary in [
            "postgres://postgres@unraid.cloud1ful.com:5432/postgres".to_owned(),
            "postgres://postgres@localhost:5432/cloudiful_tasks_issue_1194".to_owned(),
            "postgres://postgres@unraid.cloud1ful.com:5433/cloudiful_tasks_issue_1194".to_owned(),
            "postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_issue_1194?host=/tmp"
                .to_owned(),
            "postgres://postgres@unraid.cloud1ful.com:5432/cloudiful_tasks_issue_1194?options=-csearch_path%3Dpublic"
                .to_owned(),
        ] {
            assert!(
                fixture_config_from_values(Some(&primary), Some(&sibling)).is_err(),
                "must refuse {primary}"
            );
        }
    }

    #[test]
    fn identity_requires_the_exact_issue_name_marker_and_endpoint() {
        let pair = pair();
        let options = parse_options(&url(pair.primary())).unwrap();
        let row = identity(&pair);
        assert!(validate_identity(&row, &options, pair.primary(), pair.primary_marker()).is_ok());

        let mut wrong = identity(&pair);
        wrong.database_name = Some("postgres".to_owned());
        assert!(
            validate_identity(&wrong, &options, pair.primary(), pair.primary_marker()).is_err()
        );

        let mut wrong = identity(&pair);
        wrong.database_comment = Some("another fixture".to_owned());
        assert!(
            validate_identity(&wrong, &options, pair.primary(), pair.primary_marker()).is_err()
        );

        let mut wrong = identity(&pair);
        wrong.user_name = Some("someone-else".to_owned());
        assert!(
            validate_identity(&wrong, &options, pair.primary(), pair.primary_marker()).is_err()
        );

        let mut wrong = identity(&pair);
        wrong.server_address = None;
        assert!(
            validate_identity(&wrong, &options, pair.primary(), pair.primary_marker()).is_err()
        );

        let mut wrong = identity(&pair);
        wrong.server_address = Some("127.0.0.1".to_owned());
        assert!(
            validate_identity(&wrong, &options, pair.primary(), pair.primary_marker()).is_err()
        );

        let mut wrong = identity(&pair);
        wrong.server_port = Some("5434".to_owned());
        assert!(
            validate_identity(&wrong, &options, pair.primary(), pair.primary_marker()).is_err()
        );
    }
}
