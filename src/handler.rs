//! The handler contract of a worker.
//!
//! A handler receives the opaque payload of a claimed task and the identity of
//! the attempt that runs it. It reports progress, observes the cancellation of
//! its attempt and returns the outcome of the task.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};
use crate::model::{Attempt, Failure, ProgressUpdate, TaskId, TaskKind};
use crate::queue::Queue;

/// The outcome of a handler.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// The task finished successfully, with an optional result.
    Completed(Option<Value>),
    /// The task failed.
    Failed(Failure),
    /// The handler already committed the terminal state of its attempt.
    ///
    /// The worker verifies that the exact captured attempt was sealed as
    /// succeeded, failed or cancelled before it skips its own terminal write.
    /// An unsealed attempt is handled as a failure, and a lost lease is never
    /// written to a newer attempt.
    ExternallyCompleted,
}

impl Outcome {
    /// The outcome of a successful task.
    pub fn completed(result: Option<Value>) -> Self {
        Self::Completed(result)
    }

    /// The outcome of a failed task.
    pub fn failed(failure: Failure) -> Self {
        Self::Failed(failure)
    }
}

/// The error of a handler.
///
/// The error never carries the payload of the task, so it can be logged
/// safely.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct HandlerError {
    message: String,
}

impl HandlerError {
    /// Builds an error with a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// The message of the error.
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// A handler of one task kind.
#[async_trait]
pub trait TaskHandler: Send + Sync {
    /// Handles a claimed task.
    ///
    /// The handler must return when the cancellation token of the context is
    /// cancelled, and its side effects must be idempotent: the attempt may run
    /// again after a lost lease.
    async fn handle(&self, context: TaskContext) -> std::result::Result<Outcome, HandlerError>;
}

/// The context of a running attempt.
#[derive(Clone)]
pub struct TaskContext {
    queue: Queue,
    attempt: Attempt,
    task_id: TaskId,
    kind: TaskKind,
    payload: Value,
    cancel: CancellationToken,
}

impl TaskContext {
    pub(crate) fn new(
        queue: Queue,
        attempt: Attempt,
        task_id: TaskId,
        kind: TaskKind,
        payload: Value,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            queue,
            attempt,
            task_id,
            kind,
            payload,
            cancel,
        }
    }

    /// The task of the attempt.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// The attempt that runs the task.
    pub fn attempt(&self) -> &Attempt {
        &self.attempt
    }

    /// The number of the attempt, starting at one.
    pub fn attempt_number(&self) -> u32 {
        self.attempt.attempt_number()
    }

    /// The fencing token of the attempt.
    pub fn fencing_token(&self) -> crate::model::FencingToken {
        self.attempt.fencing_token()
    }

    /// The kind of the task.
    pub fn kind(&self) -> &TaskKind {
        &self.kind
    }

    /// The opaque payload of the task.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// The token that is cancelled when the attempt must stop.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Whether the attempt was asked to stop.
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }

    /// Writes the progress report of the attempt.
    ///
    /// The report belongs to the attempt, a new attempt starts without
    /// progress. When the attempt lost its lease, the report is refused and
    /// the attempt is asked to stop.
    pub async fn progress(&self, report: &ProgressUpdate) -> Result<()> {
        report.validate()?;
        match self.queue.report_progress(&self.attempt, report).await {
            Ok(()) => Ok(()),
            Err(error @ Error::LeaseLost { .. }) => {
                self.cancel.cancel();
                Err(error)
            }
            Err(error) => Err(error),
        }
    }
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("task_id", &self.task_id)
            .field("attempt", &self.attempt)
            .field("kind", &self.kind)
            .field("payload", &"<opaque>")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}

/// A registered handler.
pub type SharedHandler = Arc<dyn TaskHandler>;
