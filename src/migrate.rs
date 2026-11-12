//! Explicit schema bootstrap and schema compatibility checks.
//!
//! The library owns one PostgreSQL schema. The schema is created by
//! [`migrate`] and never by opening a queue or by starting a worker. A schema
//! that exists without the ownership marker of this library is refused instead
//! of being adopted.

use std::time::Instant;

use sea_orm::{
    ConnectionTrait, DatabaseConnection, EntityTrait, QueryResult, Statement, TransactionTrait,
};
use sha2::{Digest, Sha384};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::entity::schema_ownership;
use crate::error::{Error, Result};
use crate::sql;

/// Schema owned by this library.
pub const SCHEMA_NAME: &str = "cloudiful_tasks";

/// Identifier stored in the ownership marker.
pub const LIBRARY_ID: &str = "cloudiful-tasks";

/// Schema protocol version implemented by this build.
pub const PROTOCOL_VERSION: i32 = 3;

/// Table holding the ownership marker of the schema.
const OWNERSHIP_TABLE: &str = "schema_ownership";

/// Table holding the migration ledger of this library.
const LEDGER_TABLE: &str = "_sqlx_migrations";

const SCHEMA_EXISTS: &str = include_str!("../sql/migration_schema_exists.sql");
const SCHEMA_RELATIONS: &str = include_str!("../sql/migration_schema_relations.sql");
const LEDGER: &str = include_str!("../sql/migration_ledger.sql");
const CURRENT_DATABASE: &str = include_str!("../sql/migration_current_database.sql");
const ADVISORY_XACT_LOCK: &str = include_str!("../sql/migration_advisory_xact_lock.sql");
const RECORD_MIGRATION: &str = include_str!("../sql/migration_record.sql");
const CREATE_LEDGER: &str = include_str!("../sql/migration_create_ledger.sql");
const CREATE_SCHEMA: &str = include_str!("../sql/migration_create_schema.sql");

/// One immutable migration of the library.
struct Migration {
    version: i64,
    description: &'static str,
    sql: &'static str,
}

/// The migrations of the library, in application order.
///
/// The descriptors mirror the files in `migrations/`; their SQL is embedded so
/// that the checksums of the legacy ledger can be verified without the SQLx
/// migration engine.
const MIGRATIONS: &[Migration] = &[
    Migration {
        version: 20261021032949,
        description: "init_schema",
        sql: include_str!("../migrations/20261021032949_init_schema.sql"),
    },
    Migration {
        version: 20261023175252,
        description: "queue_policies",
        sql: include_str!("../migrations/20261023175252_queue_policies.sql"),
    },
    Migration {
        version: 20261106211859,
        description: "task_pause",
        sql: include_str!("../migrations/20261106211859_task_pause.sql"),
    },
];

/// Information about a schema that is owned by this library.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    protocol_version: i32,
    instance_uuid: Uuid,
    installed_at: OffsetDateTime,
    applied_versions: Vec<i64>,
}

impl SchemaInfo {
    /// Protocol version of the schema.
    pub fn protocol_version(&self) -> i32 {
        self.protocol_version
    }

    /// Identifier of the schema instance. It stays stable for the lifetime of
    /// the database.
    pub fn instance_uuid(&self) -> Uuid {
        self.instance_uuid
    }

    /// Time at which the schema was installed.
    pub fn installed_at(&self) -> OffsetDateTime {
        self.installed_at
    }

    /// Versions of the migrations that are applied to the schema.
    pub fn applied_versions(&self) -> &[i64] {
        &self.applied_versions
    }
}

/// The result of a successful [`migrate`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationOutcome {
    info: SchemaInfo,
    applied: bool,
}

impl MigrationOutcome {
    /// Information about the schema after the migration.
    pub fn info(&self) -> &SchemaInfo {
        &self.info
    }

    /// Protocol version of the schema.
    pub fn protocol_version(&self) -> i32 {
        self.info.protocol_version()
    }

    /// Identifier of the schema instance.
    pub fn instance_uuid(&self) -> Uuid {
        self.info.instance_uuid()
    }

    /// Versions of the migrations that are applied to the schema.
    pub fn applied_versions(&self) -> &[i64] {
        self.info.applied_versions()
    }

    /// Whether this call applied a migration that was not applied before.
    pub fn applied(&self) -> bool {
        self.applied
    }
}

/// Creates the schema of this library and applies its migrations.
///
/// The call is idempotent: running it against an up to date schema changes
/// nothing. It is serialized per database, so concurrent calls apply the
/// migrations exactly once.
///
/// The call refuses a schema that exists without the ownership marker of this
/// library, and a schema that was written by a newer, incompatible version of
/// the library. An owned schema of an older supported protocol version is
/// upgraded to the current version.
///
/// # Errors
///
/// Returns [`Error::ForeignSchema`] for a schema that this library does not
/// own, [`Error::UnsupportedSchemaVersion`] for a schema of a newer protocol
/// version and [`Error::Migration`] when the migration engine fails. A dirty
/// migration ledger is refused before any pending migration is applied.
pub async fn migrate(db: &DatabaseConnection) -> Result<MigrationOutcome> {
    let transaction = db.begin().await?;
    let outcome = migrate_locked(&transaction).await;
    match outcome {
        Ok(outcome) => {
            transaction.commit().await?;
            Ok(outcome)
        }
        Err(error) => {
            transaction.rollback().await.ok();
            Err(error)
        }
    }
}

/// Checks that the database holds a schema that is owned by this library.
///
/// The check never writes to the database. It refuses a missing schema, a
/// schema that this library does not own, a schema of an incompatible
/// protocol version and a schema whose applied migrations no longer match the
/// migration source.
///
/// # Errors
///
/// Returns [`Error::SchemaMissing`], [`Error::ForeignSchema`],
/// [`Error::UnsupportedSchemaVersion`], [`Error::OutdatedSchemaVersion`],
/// [`Error::DirtyMigration`] or [`Error::ChecksumMismatch`] when the database
/// is not ready for use.
pub async fn check_schema(db: &DatabaseConnection) -> Result<SchemaInfo> {
    if !schema_exists(db).await? {
        return Err(Error::SchemaMissing {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let relations = schema_relations(db).await?;
    if !relations.iter().any(|name| name == OWNERSHIP_TABLE) {
        return Err(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    read_schema_info(db).await
}

async fn migrate_locked(db: &impl ConnectionTrait) -> Result<MigrationOutcome> {
    let database = current_database(db).await?;
    let lock_key = migrate_lock_key(&database);
    db.query_one_raw(sql::statement(ADVISORY_XACT_LOCK, vec![lock_key.into()]))
        .await?;

    ensure_ownership(db).await?;
    let rows = existing_ledger_rows(db).await?;
    let before = rows.len();
    let applied = verify_ledger_rows(&rows).map_err(migration_integrity)?;
    if !schema_exists(db).await? {
        db.execute_unprepared(CREATE_SCHEMA).await?;
    }
    db.execute_unprepared(CREATE_LEDGER).await?;

    let mut changed = false;
    for migration in MIGRATIONS {
        if applied.contains(&migration.version) {
            continue;
        }
        let checksum = checksum(migration.sql);
        let started = Instant::now();
        db.execute_unprepared(migration.sql)
            .await
            .map_err(|error| Error::Migration(error.to_string()))?;
        let execution_time = i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX);
        db.execute_raw(sql::statement(
            RECORD_MIGRATION,
            vec![
                migration.version.into(),
                migration.description.into(),
                checksum.to_vec().into(),
                execution_time.into(),
            ],
        ))
        .await
        .map_err(|error| Error::Migration(error.to_string()))?;
        changed = true;
    }

    let info = read_schema_info(db).await?;
    Ok(MigrationOutcome {
        applied: changed || info.applied_versions().len() > before,
        info,
    })
}

/// Refuses a schema that this library does not own.
///
/// A schema that does not exist yet belongs to the bootstrap of this library.
/// A schema that exists belongs to this library only when it carries the
/// ownership marker, an empty schema and a schema that holds the migration
/// ledger alone are foreign schemas.
async fn ensure_ownership(db: &impl ConnectionTrait) -> Result<()> {
    if !schema_exists(db).await? {
        return Ok(());
    }
    let relations = schema_relations(db).await?;
    if !relations.iter().any(|name| name == OWNERSHIP_TABLE) {
        return Err(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let marker = read_ownership(db).await?.ok_or(Error::ForeignSchema {
        schema: SCHEMA_NAME.to_owned(),
    })?;
    validate_owner(&marker)
}

async fn read_schema_info(db: &impl ConnectionTrait) -> Result<SchemaInfo> {
    let marker = read_ownership(db).await?.ok_or(Error::ForeignSchema {
        schema: SCHEMA_NAME.to_owned(),
    })?;
    if marker.library != LIBRARY_ID {
        return Err(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let applied_versions = verify_ledger(db).await?;
    validate_marker(&marker)?;
    Ok(SchemaInfo {
        protocol_version: marker.protocol_version,
        instance_uuid: marker.instance_uuid,
        installed_at: marker.installed_at,
        applied_versions,
    })
}

/// Refuses a schema that another library owns or a schema of a newer protocol.
///
/// Callers that migrate accept an owned older protocol version, because the
/// migrations upgrade it to the current version. Callers that read accept only
/// the exact supported version.
fn validate_owner(marker: &schema_ownership::Model) -> Result<()> {
    if marker.library != LIBRARY_ID {
        return Err(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    if marker.protocol_version > PROTOCOL_VERSION {
        return Err(Error::UnsupportedSchemaVersion {
            found: marker.protocol_version,
            supported: PROTOCOL_VERSION,
        });
    }
    Ok(())
}

fn validate_marker(marker: &schema_ownership::Model) -> Result<()> {
    validate_owner(marker)?;
    if marker.protocol_version < PROTOCOL_VERSION {
        return Err(Error::OutdatedSchemaVersion {
            found: marker.protocol_version,
            supported: PROTOCOL_VERSION,
        });
    }
    Ok(())
}

async fn schema_exists(db: &impl ConnectionTrait) -> Result<bool> {
    Ok(
        fetch_scalar::<bool>(db, sql::statement(SCHEMA_EXISTS, vec![SCHEMA_NAME.into()]))
            .await?
            .unwrap_or(false),
    )
}

async fn schema_relations(db: &impl ConnectionTrait) -> Result<Vec<String>> {
    let rows = db
        .query_all_raw(sql::statement(SCHEMA_RELATIONS, vec![SCHEMA_NAME.into()]))
        .await?;
    rows.into_iter()
        .map(|row| row.try_get_by::<String, usize>(0).map_err(Error::from))
        .collect()
}

async fn read_ownership(db: &impl ConnectionTrait) -> Result<Option<schema_ownership::Model>> {
    Ok(schema_ownership::Entity::find().one(db).await?)
}

async fn existing_ledger_rows(db: &impl ConnectionTrait) -> Result<Vec<LedgerRow>> {
    let relations = schema_relations(db).await?;
    if !relations.iter().any(|name| name == LEDGER_TABLE) {
        return Ok(Vec::new());
    }
    ledger_rows(db).await
}

fn migration_integrity(error: Error) -> Error {
    match error {
        Error::ChecksumMismatch { version } => Error::Migration(format!(
            "applied migration {version} does not match the migration source"
        )),
        Error::UnknownAppliedMigration { version } => Error::Migration(format!(
            "schema contains unknown applied migration {version}"
        )),
        Error::DirtyMigration { version } => {
            Error::Migration(format!("schema contains dirty migration {version}"))
        }
        other => other,
    }
}

async fn ledger_rows(db: &impl ConnectionTrait) -> Result<Vec<LedgerRow>> {
    let rows = db.query_all_raw(sql::statement(LEDGER, vec![])).await?;
    rows.into_iter().map(LedgerRow::from_row).collect()
}

async fn verify_ledger(db: &impl ConnectionTrait) -> Result<Vec<i64>> {
    let rows = ledger_rows(db).await?;
    verify_ledger_rows(&rows)
}

fn verify_ledger_rows(rows: &[LedgerRow]) -> Result<Vec<i64>> {
    if let Some(row) = rows.iter().find(|row| !row.success) {
        return Err(Error::DirtyMigration {
            version: row.version,
        });
    }
    let mut versions = Vec::with_capacity(rows.len());
    for row in rows {
        let migration = MIGRATIONS
            .iter()
            .find(|migration| migration.version == row.version)
            .ok_or(Error::UnknownAppliedMigration {
                version: row.version,
            })?;
        if checksum(migration.sql).as_slice() != row.checksum.as_slice() {
            return Err(Error::ChecksumMismatch {
                version: row.version,
            });
        }
        versions.push(row.version);
    }
    Ok(versions)
}

async fn current_database(db: &impl ConnectionTrait) -> Result<String> {
    fetch_scalar::<String>(db, sql::statement(CURRENT_DATABASE, vec![]))
        .await?
        .ok_or(Error::Validation {
            field: "database",
            reason: "the database did not report its name".to_owned(),
        })
}

/// Derives the advisory lock key of a database.
///
/// The key is stable for a database name and differs between databases, so
/// migrations of different databases do not block each other.
fn migrate_lock_key(database: &str) -> i64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    const SALT: u64 = 0x9e37_79b9_7f4a_7c15;

    let mut hash = OFFSET ^ SALT;
    for byte in database.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash as i64
}

fn checksum(sql: &str) -> [u8; 48] {
    let digest = Sha384::digest(sql.as_bytes());
    let mut checksum = [0_u8; 48];
    checksum.copy_from_slice(&digest[..]);
    checksum
}

struct LedgerRow {
    version: i64,
    checksum: Vec<u8>,
    success: bool,
}

impl LedgerRow {
    fn from_row(row: QueryResult) -> Result<Self> {
        Ok(Self {
            version: row.try_get_by::<i64, usize>(0)?,
            checksum: row.try_get_by::<Vec<u8>, usize>(1)?,
            success: row.try_get_by::<bool, usize>(2)?,
        })
    }
}

async fn fetch_scalar<T>(
    connection: &impl ConnectionTrait,
    statement: Statement,
) -> Result<Option<T>>
where
    T: sea_orm::TryGetable,
{
    match connection.query_one_raw(statement).await? {
        Some(row) => Ok(Some(row.try_get_by::<T, usize>(0)?)),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn marker(library: &str, protocol_version: i32) -> schema_ownership::Model {
        schema_ownership::Model {
            singleton: true,
            library: library.to_owned(),
            protocol_version,
            instance_uuid: Uuid::new_v4(),
            installed_at: OffsetDateTime::UNIX_EPOCH,
            updated_at: OffsetDateTime::UNIX_EPOCH,
        }
    }

    fn ledger_row(version: i64, checksum_bytes: Vec<u8>, success: bool) -> LedgerRow {
        LedgerRow {
            version,
            checksum: checksum_bytes,
            success,
        }
    }

    fn matching_row(index: usize) -> LedgerRow {
        let migration = &MIGRATIONS[index];
        ledger_row(migration.version, checksum(migration.sql).to_vec(), true)
    }

    #[test]
    fn lock_key_is_stable_and_database_specific() {
        assert_eq!(
            migrate_lock_key("cloudiful_tasks_test"),
            migrate_lock_key("cloudiful_tasks_test")
        );
        assert_ne!(
            migrate_lock_key("cloudiful_tasks_test"),
            migrate_lock_key("cloudiful_tasks_dev")
        );
    }

    #[test]
    fn a_ledger_of_the_current_migrations_is_accepted() {
        let rows: Vec<LedgerRow> = (0..MIGRATIONS.len()).map(matching_row).collect();

        let versions = verify_ledger_rows(&rows).expect("the ledger must be accepted");

        assert_eq!(
            versions,
            MIGRATIONS
                .iter()
                .map(|migration| migration.version)
                .collect::<Vec<i64>>()
        );
    }

    #[test]
    fn a_dirty_ledger_row_is_refused_before_any_other_check() {
        let known = matching_row(0);
        let dirty_known = ledger_row(known.version, known.checksum.clone(), false);
        let error = verify_ledger_rows(&[dirty_known]).expect_err("a dirty row must be refused");
        assert!(
            matches!(error, Error::DirtyMigration { version } if version == known.version),
            "unexpected error: {error}"
        );

        let unknown = ledger_row(1, vec![0_u8; 48], false);
        let error = verify_ledger_rows(&[unknown]).expect_err("a dirty row must be refused");
        assert!(
            matches!(error, Error::DirtyMigration { .. }),
            "a dirty row wins over an unknown version: {error}"
        );
    }

    #[test]
    fn an_unknown_applied_migration_is_refused() {
        let row = ledger_row(1, vec![0_u8; 48], true);

        let error = verify_ledger_rows(&[row]).expect_err("an unknown version must be refused");

        assert!(
            matches!(error, Error::UnknownAppliedMigration { version } if version == 1),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_drifted_checksum_is_refused() {
        let migration = &MIGRATIONS[0];
        let row = ledger_row(migration.version, vec![0_u8; 48], true);

        let error = verify_ledger_rows(&[row]).expect_err("a drifted checksum must be refused");

        assert!(
            matches!(
                error,
                Error::ChecksumMismatch { version } if version == migration.version
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_foreign_ownership_marker_is_refused() {
        let error = validate_owner(&marker("other-library", PROTOCOL_VERSION))
            .expect_err("a foreign marker must be refused");

        assert!(
            matches!(error, Error::ForeignSchema { .. }),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn a_newer_protocol_is_refused() {
        let error = validate_owner(&marker(LIBRARY_ID, PROTOCOL_VERSION + 1))
            .expect_err("a newer protocol must be refused");

        assert!(
            matches!(
                error,
                Error::UnsupportedSchemaVersion { found, supported }
                    if found == PROTOCOL_VERSION + 1 && supported == PROTOCOL_VERSION
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn an_older_protocol_is_upgraded_but_not_accepted_for_reads() {
        let older = marker(LIBRARY_ID, PROTOCOL_VERSION - 1);

        assert!(
            validate_owner(&older).is_ok(),
            "a migration accepts an owned older protocol"
        );

        let error = validate_marker(&older).expect_err("a read refuses an older protocol");
        assert!(
            matches!(
                error,
                Error::OutdatedSchemaVersion { found, supported }
                    if found == PROTOCOL_VERSION - 1 && supported == PROTOCOL_VERSION
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn the_current_protocol_is_accepted() {
        let current = marker(LIBRARY_ID, PROTOCOL_VERSION);

        assert!(validate_owner(&current).is_ok());
        assert!(validate_marker(&current).is_ok());
    }

    #[test]
    fn the_checksum_is_the_sha384_of_the_migration_source() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|byte| format!("{byte:02x}")).collect()
        }

        assert_eq!(
            hex(&checksum(MIGRATIONS[0].sql)),
            "d873e6151868b365fc1731d089a706f3f4a8289a2cb852e227555bd76bbb4ad0\
             dc4303bde49ba5b007a8f4e5e15323ec",
            "the legacy ledger stores the sha384 digest of the migration source"
        );
    }
}
