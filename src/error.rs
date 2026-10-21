//! Error types returned by the library.

use uuid::Uuid;

/// Result alias used by every fallible library call.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Errors returned by the library.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A caller supplied value failed validation.
    #[error("invalid {field}: {reason}")]
    Validation {
        /// Name of the validated field.
        field: &'static str,
        /// Why the value was rejected.
        reason: String,
    },

    /// The database rejected or failed a statement.
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    /// The migration engine failed to apply the library migrations.
    #[error("migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    /// The schema exists but is not owned by this library.
    #[error("schema `{schema}` is not owned by cloudiful-tasks")]
    ForeignSchema {
        /// Name of the offending schema.
        schema: String,
    },

    /// The schema does not exist yet.
    #[error("schema `{schema}` does not exist; call migrate() first")]
    SchemaMissing {
        /// Name of the missing schema.
        schema: String,
    },

    /// The schema was written by a newer, incompatible version of the library.
    #[error("schema protocol version {found} is newer than the supported version {supported}")]
    UnsupportedSchemaVersion {
        /// Protocol version found in the database.
        found: i32,
        /// Protocol version implemented by this build.
        supported: i32,
    },

    /// The transaction belongs to another database than the queue.
    #[error(
        "the transaction belongs to the database instance {found} but the queue uses {expected}"
    )]
    InstanceMismatch {
        /// Instance of the schema the queue was opened on.
        expected: Uuid,
        /// Instance of the schema the transaction belongs to.
        found: Uuid,
    },

    /// The schema is older than the version implemented by this library.
    #[error(
        "schema protocol version {found} is older than the supported version {supported}; run migrate()"
    )]
    OutdatedSchemaVersion {
        /// Protocol version found in the database.
        found: i32,
        /// Protocol version implemented by this build.
        supported: i32,
    },

    /// An applied migration no longer matches the migration source.
    #[error("applied migration {version} does not match the migration source")]
    ChecksumMismatch {
        /// Version of the diverging migration.
        version: i64,
    },

    /// The schema contains an applied migration this build does not know.
    #[error("schema contains unknown applied migration {version}")]
    UnknownAppliedMigration {
        /// Version of the unknown migration.
        version: i64,
    },

    /// The requested task does not exist in the given namespace and queue.
    #[error("task not found")]
    NotFound,

    /// The caller no longer owns the active lease of the task.
    #[error("lease lost for task {task_id}")]
    LeaseLost {
        /// Task whose lease was lost.
        task_id: Uuid,
    },

    /// The idempotency key was reused with a different request.
    #[error("idempotency key `{key}` was already used with a different request")]
    IdempotencyConflict {
        /// The conflicting idempotency key.
        key: String,
    },

    /// The task is not in a state that allows the requested transition.
    #[error("task {task_id} is in state {status} and cannot be {action}")]
    InvalidTaskState {
        /// Task whose state blocks the transition.
        task_id: Uuid,
        /// Current state of the task.
        status: String,
        /// The rejected transition.
        action: &'static str,
    },
}
