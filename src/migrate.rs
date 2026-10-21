//! Explicit schema bootstrap and schema compatibility checks.
//!
//! The library owns one PostgreSQL schema. The schema is created by
//! [`migrate`] and never by opening a queue or by starting a worker. A schema
//! that exists without the ownership marker of this library is refused instead
//! of being adopted.

use chrono::{DateTime, Utc};
use sqlx::migrate::Migrator;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::error::{Error, Result};

/// Schema owned by this library.
pub const SCHEMA_NAME: &str = "cloudiful_tasks";

/// Identifier stored in the ownership marker.
pub const LIBRARY_ID: &str = "cloudiful-tasks";

/// Schema protocol version implemented by this build.
pub const PROTOCOL_VERSION: i32 = 1;

/// Table holding the ownership marker of the schema.
const OWNERSHIP_TABLE: &str = "schema_ownership";

/// Table holding the migration ledger of this library.
const LEDGER_TABLE: &str = "_sqlx_migrations";

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Information about a schema that is owned by this library.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    protocol_version: i32,
    instance_uuid: Uuid,
    installed_at: DateTime<Utc>,
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
    pub fn installed_at(&self) -> DateTime<Utc> {
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
/// the library.
///
/// # Errors
///
/// Returns [`Error::ForeignSchema`] for a schema that this library does not
/// own, [`Error::UnsupportedSchemaVersion`] for a schema of a newer protocol
/// version and [`Error::Migration`] when the migration engine fails.
pub async fn migrate(pool: &PgPool) -> Result<MigrationOutcome> {
    let mut connection = pool.acquire().await?;
    let result = migrate_on_connection(&mut connection).await;
    match result {
        Ok(outcome) => Ok(outcome),
        Err(error) => {
            // The session may hold an advisory lock. Closing it keeps the lock
            // out of the pool instead of leaking it to the next user.
            connection.close().await.ok();
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
/// [`Error::UnsupportedSchemaVersion`], [`Error::OutdatedSchemaVersion`] or
/// [`Error::ChecksumMismatch`] when the database is not ready for use.
pub async fn check_schema(pool: &PgPool) -> Result<SchemaInfo> {
    let mut connection = pool.acquire().await?;
    check_schema_on_connection(&mut connection).await
}

async fn migrate_on_connection(connection: &mut PgConnection) -> Result<MigrationOutcome> {
    let database = current_database(connection).await?;
    let lock_key = migrate_lock_key(&database);
    advisory_lock(connection, lock_key).await?;
    let outcome = migrate_locked(connection).await;
    advisory_unlock(connection, lock_key).await;
    outcome
}

async fn migrate_locked(connection: &mut PgConnection) -> Result<MigrationOutcome> {
    ensure_ownership(connection).await?;
    let before = applied_versions(connection).await?;
    MIGRATOR
        .run(&mut *connection)
        .await
        .map_err(Error::Migration)?;
    let info = read_schema_info(connection).await?;
    Ok(MigrationOutcome {
        applied: info.applied_versions().len() > before.len(),
        info,
    })
}

async fn check_schema_on_connection(connection: &mut PgConnection) -> Result<SchemaInfo> {
    if !schema_exists(connection).await? {
        return Err(Error::SchemaMissing {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let relations = schema_relations(connection).await?;
    if !relations.iter().any(|name| name == OWNERSHIP_TABLE) {
        return Err(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    read_schema_info(connection).await
}

/// Refuses a schema that this library does not own.
///
/// A schema that does not exist yet belongs to the bootstrap of this library.
/// A schema that exists belongs to this library only when it carries the
/// ownership marker, an empty schema and a schema that holds the migration
/// ledger alone are foreign schemas.
async fn ensure_ownership(connection: &mut PgConnection) -> Result<()> {
    if !schema_exists(connection).await? {
        return Ok(());
    }
    let relations = schema_relations(connection).await?;
    if !relations.iter().any(|name| name == OWNERSHIP_TABLE) {
        return Err(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let marker = read_ownership(connection)
        .await?
        .ok_or(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        })?;
    validate_marker(&marker)
}

async fn read_schema_info(connection: &mut PgConnection) -> Result<SchemaInfo> {
    let marker = read_ownership(connection)
        .await?
        .ok_or(Error::ForeignSchema {
            schema: SCHEMA_NAME.to_owned(),
        })?;
    validate_marker(&marker)?;
    let applied_versions = verify_ledger(connection).await?;
    Ok(SchemaInfo {
        protocol_version: marker.protocol_version,
        instance_uuid: marker.instance_uuid,
        installed_at: marker.installed_at,
        applied_versions,
    })
}

fn validate_marker(marker: &OwnershipRow) -> Result<()> {
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
    if marker.protocol_version < PROTOCOL_VERSION {
        return Err(Error::OutdatedSchemaVersion {
            found: marker.protocol_version,
            supported: PROTOCOL_VERSION,
        });
    }
    Ok(())
}

async fn schema_exists(connection: &mut PgConnection) -> Result<bool> {
    let row = sqlx::query_file!("sql/migration_schema_exists.sql", SCHEMA_NAME)
        .fetch_one(&mut *connection)
        .await?;
    Ok(row.schema_exists.unwrap_or(false))
}

async fn schema_relations(connection: &mut PgConnection) -> Result<Vec<String>> {
    let rows = sqlx::query_file_as!(
        RelationRow,
        "sql/migration_schema_relations.sql",
        SCHEMA_NAME
    )
    .fetch_all(&mut *connection)
    .await?;
    Ok(rows.into_iter().map(|row| row.relation_name).collect())
}

async fn read_ownership(connection: &mut PgConnection) -> Result<Option<OwnershipRow>> {
    Ok(
        sqlx::query_file_as!(OwnershipRow, "sql/migration_ownership.sql")
            .fetch_optional(&mut *connection)
            .await?,
    )
}

async fn applied_versions(connection: &mut PgConnection) -> Result<Vec<i64>> {
    let relations = schema_relations(connection).await?;
    if !relations.iter().any(|name| name == LEDGER_TABLE) {
        return Ok(Vec::new());
    }
    let rows = sqlx::query_file_as!(LedgerRow, "sql/migration_ledger.sql")
        .fetch_all(&mut *connection)
        .await?;
    Ok(rows.into_iter().map(|row| row.version).collect())
}

async fn verify_ledger(connection: &mut PgConnection) -> Result<Vec<i64>> {
    let rows = sqlx::query_file_as!(LedgerRow, "sql/migration_ledger.sql")
        .fetch_all(&mut *connection)
        .await?;
    let mut versions = Vec::with_capacity(rows.len());
    for row in rows {
        let migration = MIGRATOR
            .iter()
            .find(|migration| migration.version == row.version)
            .ok_or(Error::UnknownAppliedMigration {
                version: row.version,
            })?;
        if migration.checksum.as_ref() != row.checksum.as_slice() {
            return Err(Error::ChecksumMismatch {
                version: row.version,
            });
        }
        versions.push(row.version);
    }
    Ok(versions)
}

async fn current_database(connection: &mut PgConnection) -> Result<String> {
    let row = sqlx::query_file!("sql/migration_current_database.sql")
        .fetch_one(&mut *connection)
        .await?;
    row.database_name.ok_or(Error::Validation {
        field: "database",
        reason: "the database did not report its name".to_owned(),
    })
}

async fn advisory_lock(connection: &mut PgConnection, key: i64) -> Result<()> {
    sqlx::query_file!("sql/migration_advisory_lock.sql", key)
        .fetch_one(&mut *connection)
        .await?;
    Ok(())
}

async fn advisory_unlock(connection: &mut PgConnection, key: i64) {
    let _ = sqlx::query_file!("sql/migration_advisory_unlock.sql", key)
        .fetch_one(&mut *connection)
        .await;
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

#[derive(sqlx::FromRow)]
struct RelationRow {
    relation_name: String,
}

#[derive(sqlx::FromRow)]
pub(crate) struct OwnershipRow {
    pub(crate) library: String,
    pub(crate) protocol_version: i32,
    pub(crate) instance_uuid: Uuid,
    pub(crate) installed_at: DateTime<Utc>,
}

#[derive(sqlx::FromRow)]
struct LedgerRow {
    version: i64,
    checksum: Vec<u8>,
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
