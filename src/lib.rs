//! Durable PostgreSQL backed task execution.
//!
//! `cloudiful-tasks` keeps tasks, their attempts and their leases in a
//! PostgreSQL schema that is owned by this library. The schema is created
//! explicitly through [`migrate()`]; opening a queue or running a worker never
//! creates or alters schema.
//!
//! # Isolation
//!
//! Every operation is scoped by a logical namespace and a queue name. The
//! namespace is a logical isolation mechanism inside one database, not a
//! security boundary: strong isolation requires a separate database or a
//! separate database role.
//!
//! # Durability
//!
//! Tasks are executed at least once. A worker that loses its lease can still
//! run to completion, so handlers must be idempotent and callers must fence
//! their own side effects with the attempt fencing token.
//!
//! The current release implements the data model, the configuration contract,
//! the explicit schema bootstrap and the durable queue store. The worker
//! runtime is not implemented yet.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod backoff;
pub mod config;
pub mod error;
pub mod handler;
pub mod idempotency;
pub mod migrate;
pub mod model;
pub mod progress;
pub mod queue;
pub mod store;
pub mod wake;
pub mod worker;

pub use crate::config::WorkerConfig;
pub use crate::error::{Error, Result};
pub use crate::handler::{HandlerError, Outcome, TaskContext, TaskHandler};
pub use crate::idempotency::canonical_request_spec;
pub use crate::migrate::{MigrationOutcome, SchemaInfo, check_schema, migrate};
pub use crate::model::{
    Attempt, AttemptId, AttemptStatus, Claim, EnqueueOutcome, EnqueueRequest, Failure,
    FencingToken, ListCursor, Namespace, ProgressUpdate, QueueName, RetryPolicy, Task, TaskId,
    TaskKind, TaskStatus,
};
pub use crate::queue::{MAX_ATTEMPT_LIMIT, MAX_LIST_LIMIT, MAX_RECOVERY_LIMIT, Queue};
pub use crate::wake::{WAKE_CHANNEL, WakeListener, Wakeup};
pub use crate::worker::{Worker, WorkerBuilder};
pub use tokio_util::sync::CancellationToken;
