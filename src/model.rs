//! Public data model: validated names, task states, requests and progress.

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use uuid::Uuid;

use crate::error::{Error, Result};

/// Maximum length of a namespace, queue name or task kind.
pub const MAX_NAME_LENGTH: usize = 128;

/// Maximum length of an idempotency key.
pub const MAX_IDEMPOTENCY_KEY_LENGTH: usize = 255;

/// Maximum length of a progress stage.
pub const MAX_STAGE_LENGTH: usize = 128;

/// Maximum length of a progress unit.
pub const MAX_UNIT_LENGTH: usize = 32;

/// Upper bound for a retry delay.
pub const MAX_RETRY_DELAY: Duration = Duration::from_secs(24 * 60 * 60);

fn validate_name(field: &'static str, value: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::Validation {
            field,
            reason: "must not be empty".to_owned(),
        });
    }
    if value.len() > MAX_NAME_LENGTH {
        return Err(Error::Validation {
            field,
            reason: format!("must be at most {MAX_NAME_LENGTH} bytes"),
        });
    }
    if let Some(found) = value
        .chars()
        .find(|c| !matches!(c, 'A'..='Z' | 'a'..='z' | '0'..='9' | '_' | '.' | ':' | '-'))
    {
        return Err(Error::Validation {
            field,
            reason: format!(
                "contains unsupported character {found:?}; allowed characters are A-Z, a-z, 0-9, _, ., : and -"
            ),
        });
    }
    Ok(())
}

fn validate_idempotency_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(Error::Validation {
            field: "idempotency_key",
            reason: "must not be empty".to_owned(),
        });
    }
    if key.len() > MAX_IDEMPOTENCY_KEY_LENGTH {
        return Err(Error::Validation {
            field: "idempotency_key",
            reason: format!("must be at most {MAX_IDEMPOTENCY_KEY_LENGTH} bytes"),
        });
    }
    if key.chars().any(char::is_control) {
        return Err(Error::Validation {
            field: "idempotency_key",
            reason: "must not contain control characters".to_owned(),
        });
    }
    Ok(())
}

macro_rules! validated_name {
    ($name:ident, $field:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            /// Validates `value` and returns the name.
            pub fn new(value: &str) -> Result<Self> {
                validate_name($field, value)?;
                Ok(Self(value.to_owned()))
            }

            /// Returns the name as a string slice.
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(
                &self,
                serializer: S,
            ) -> std::result::Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                $name::new(&value).map_err(D::Error::custom)
            }
        }
    };
}

validated_name!(
    Namespace,
    "namespace",
    "A validated logical namespace. Namespaces isolate queues inside one database."
);
validated_name!(
    QueueName,
    "queue",
    "A validated queue name. A queue is always addressed together with its namespace."
);
validated_name!(
    TaskKind,
    "kind",
    "A validated task kind. Workers dispatch on the kind of the claimed task."
);

/// Lifecycle state of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// The task waits to be claimed.
    Queued,
    /// An attempt of the task is running.
    Running,
    /// A cancellation was requested for a running task.
    CancelRequested,
    /// The task finished successfully.
    Succeeded,
    /// The task failed.
    Failed,
    /// The task was cancelled.
    Cancelled,
}

impl TaskStatus {
    /// The value stored in the database.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::CancelRequested => "cancel_requested",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Parses a value stored in the database.
    pub fn from_db(value: &str) -> Result<Self> {
        match value {
            "queued" => Ok(Self::Queued),
            "running" => Ok(Self::Running),
            "cancel_requested" => Ok(Self::CancelRequested),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            other => Err(Error::Validation {
                field: "status",
                reason: format!("unknown task status `{other}`"),
            }),
        }
    }

    /// Whether the task reached a terminal state.
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Cancelled)
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle state of an attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// The attempt holds the lease of its task.
    Running,
    /// The attempt finished successfully.
    Succeeded,
    /// The attempt failed.
    Failed,
    /// The attempt was cancelled.
    Cancelled,
    /// The attempt lost its lease and was sealed by recovery.
    Interrupted,
}

impl AttemptStatus {
    /// The value stored in the database.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Interrupted => "interrupted",
        }
    }

    /// Parses a value stored in the database.
    pub fn from_db(value: &str) -> Result<Self> {
        match value {
            "running" => Ok(Self::Running),
            "succeeded" => Ok(Self::Succeeded),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(Error::Validation {
                field: "status",
                reason: format!("unknown attempt status `{other}`"),
            }),
        }
    }

    /// Whether the attempt reached a sealed state.
    pub const fn is_sealed(self) -> bool {
        !matches!(self, Self::Running)
    }
}

impl fmt::Display for AttemptStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Bounded exponential retry policy of a task.
///
/// A policy with `max_attempts == 1` never reruns the task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    max_attempts: u32,
    initial_delay: Duration,
    max_delay: Duration,
}

impl RetryPolicy {
    /// A policy that never retries the task.
    pub fn none() -> Self {
        Self {
            max_attempts: 1,
            initial_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
        }
    }

    /// Creates a validated retry policy.
    ///
    /// `max_attempts` counts the first execution, so a value of `1` disables
    /// automatic retries. The delay before a retry grows exponentially from
    /// `initial_delay` and is capped at `max_delay`.
    pub fn new(max_attempts: u32, initial_delay: Duration, max_delay: Duration) -> Result<Self> {
        let policy = Self {
            max_attempts,
            initial_delay,
            max_delay,
        };
        policy.validate()?;
        Ok(policy)
    }

    /// Total number of attempts a task may reach.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// Delay before the first retry.
    pub fn initial_delay(&self) -> Duration {
        self.initial_delay
    }

    /// Upper bound of the retry delay.
    pub fn max_delay(&self) -> Duration {
        self.max_delay
    }

    fn validate(&self) -> Result<()> {
        if self.max_attempts == 0 {
            return Err(Error::Validation {
                field: "max_attempts",
                reason: "must be at least 1".to_owned(),
            });
        }
        if self.max_delay > MAX_RETRY_DELAY {
            return Err(Error::Validation {
                field: "max_delay",
                reason: format!("must be at most {MAX_RETRY_DELAY:?}"),
            });
        }
        if self.initial_delay > self.max_delay {
            return Err(Error::Validation {
                field: "initial_delay",
                reason: "must not exceed max_delay".to_owned(),
            });
        }
        Ok(())
    }
}

impl Serialize for RetryPolicy {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;

        let mut state = serializer.serialize_struct("RetryPolicy", 3)?;
        state.serialize_field("max_attempts", &self.max_attempts)?;
        state.serialize_field("initial_delay_ns", &self.initial_delay.as_nanos())?;
        state.serialize_field("max_delay_ns", &self.max_delay.as_nanos())?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for RetryPolicy {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Repr {
            max_attempts: u32,
            initial_delay_ns: u64,
            max_delay_ns: u64,
        }

        let repr = Repr::deserialize(deserializer)?;
        RetryPolicy::new(
            repr.max_attempts,
            Duration::from_nanos(repr.initial_delay_ns),
            Duration::from_nanos(repr.max_delay_ns),
        )
        .map_err(D::Error::custom)
    }
}

/// A request to enqueue a task.
///
/// The payload is opaque JSON owned by the caller. The library never
/// interprets or logs it.
#[derive(Debug, Clone, PartialEq)]
pub struct EnqueueRequest {
    kind: TaskKind,
    payload: Value,
    idempotency_key: Option<String>,
    priority: i32,
    available_at: Option<DateTime<Utc>>,
    retry: RetryPolicy,
}

impl EnqueueRequest {
    /// Creates a request for `kind` carrying an opaque `payload`.
    pub fn new(kind: TaskKind, payload: Value) -> Self {
        Self {
            kind,
            payload,
            idempotency_key: None,
            priority: 0,
            available_at: None,
            retry: RetryPolicy::none(),
        }
    }

    /// Sets the idempotency key of the request.
    ///
    /// Within one namespace and queue the key identifies the request: the same
    /// key with the same request returns the existing task, the same key with a
    /// different request is a conflict.
    pub fn with_idempotency_key(mut self, key: impl Into<String>) -> Result<Self> {
        let key = key.into();
        validate_idempotency_key(&key)?;
        self.idempotency_key = Some(key);
        Ok(self)
    }

    /// Sets the priority of the task. Higher priorities are claimed first.
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the earliest time at which the task may be claimed.
    pub fn with_available_at(mut self, available_at: DateTime<Utc>) -> Self {
        self.available_at = Some(available_at);
        self
    }

    /// Sets the retry policy of the task.
    pub fn with_retry_policy(mut self, retry: RetryPolicy) -> Self {
        self.retry = retry;
        self
    }

    /// The kind of the task.
    pub fn kind(&self) -> &TaskKind {
        &self.kind
    }

    /// The opaque payload of the task.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// The idempotency key of the request, if any.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// The priority of the task.
    pub fn priority(&self) -> i32 {
        self.priority
    }

    /// The earliest time at which the task may be claimed, if any.
    pub fn available_at(&self) -> Option<DateTime<Utc>> {
        self.available_at
    }

    /// The retry policy of the task.
    pub fn retry_policy(&self) -> &RetryPolicy {
        &self.retry
    }
}

/// Builds the deterministic request specification of an enqueue request.
///
/// The specification covers every immutable part of the request. Two requests
/// with the same specification are the same intent, independent of the order
/// of the keys of the payload and of the moment the request was made.
pub use crate::idempotency::canonical_request_spec;

/// A structured progress report of a running attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProgressUpdate {
    stage: String,
    completed: Option<u64>,
    total: Option<u64>,
    unit: Option<String>,
}

impl<'de> Deserialize<'de> for ProgressUpdate {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Repr {
            stage: String,
            completed: Option<u64>,
            total: Option<u64>,
            unit: Option<String>,
        }

        let repr = Repr::deserialize(deserializer)?;
        let progress = Self {
            stage: repr.stage,
            completed: repr.completed,
            total: repr.total,
            unit: repr.unit,
        };
        progress.validate().map_err(D::Error::custom)?;
        Ok(progress)
    }
}

impl ProgressUpdate {
    /// Creates a progress report for `stage`.
    pub fn new(stage: impl Into<String>) -> Self {
        Self {
            stage: stage.into(),
            completed: None,
            total: None,
            unit: None,
        }
    }

    /// Sets the number of completed units.
    pub fn with_completed(mut self, completed: u64) -> Self {
        self.completed = Some(completed);
        self
    }

    /// Sets the total number of units of the task.
    pub fn with_total(mut self, total: u64) -> Self {
        self.total = Some(total);
        self
    }

    /// Sets the unit the counters refer to.
    pub fn with_unit(mut self, unit: impl Into<String>) -> Self {
        self.unit = Some(unit.into());
        self
    }

    /// The stage of the report.
    pub fn stage(&self) -> &str {
        &self.stage
    }

    /// The number of completed units, if known.
    pub fn completed(&self) -> Option<u64> {
        self.completed
    }

    /// The total number of units, if known.
    pub fn total(&self) -> Option<u64> {
        self.total
    }

    /// The unit the counters refer to, if any.
    pub fn unit(&self) -> Option<&str> {
        self.unit.as_deref()
    }

    /// The completion in percent, if the total is known.
    ///
    /// An unknown or empty total has no percentage.
    pub fn percent(&self) -> Option<f64> {
        let total = self.total?;
        if total == 0 {
            return None;
        }
        let completed = self.completed?;
        Some(completed as f64 / total as f64 * 100.0)
    }

    /// Validates the bounds and lengths of the report.
    pub fn validate(&self) -> Result<()> {
        if self.stage.is_empty() {
            return Err(Error::Validation {
                field: "stage",
                reason: "must not be empty".to_owned(),
            });
        }
        if self.stage.len() > MAX_STAGE_LENGTH {
            return Err(Error::Validation {
                field: "stage",
                reason: format!("must be at most {MAX_STAGE_LENGTH} bytes"),
            });
        }
        if let Some(unit) = &self.unit
            && unit.len() > MAX_UNIT_LENGTH
        {
            return Err(Error::Validation {
                field: "unit",
                reason: format!("must be at most {MAX_UNIT_LENGTH} bytes"),
            });
        }
        if let (Some(completed), Some(total)) = (self.completed, self.total)
            && completed > total
        {
            return Err(Error::Validation {
                field: "completed",
                reason: format!("must not exceed total {total}"),
            });
        }
        Ok(())
    }
}

macro_rules! uuid_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl $name {
            /// Creates a new random identifier.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }

            /// Wraps an existing UUID.
            pub const fn from_uuid(id: Uuid) -> Self {
                Self(id)
            }

            /// Returns the wrapped UUID.
            pub const fn as_uuid(&self) -> Uuid {
                self.0
            }
        }

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<Uuid> for $name {
            fn from(id: Uuid) -> Self {
                Self(id)
            }
        }
    };
}

uuid_newtype!(TaskId, "Identifier of a task.");
uuid_newtype!(AttemptId, "Identifier of an attempt.");
uuid_newtype!(
    FencingToken,
    "Fencing token of an attempt. Every attempt receives a fresh, unpredictable token."
);

/// A task of a queue.
///
/// The payload and the result belong to the caller. They are opaque data that
/// the library never interprets, never logs and never prints.
#[derive(Clone, PartialEq)]
pub struct Task {
    pub(crate) id: TaskId,
    pub(crate) namespace: Namespace,
    pub(crate) queue: QueueName,
    pub(crate) kind: TaskKind,
    pub(crate) payload: Value,
    pub(crate) result: Option<Value>,
    pub(crate) status: TaskStatus,
    pub(crate) priority: i32,
    pub(crate) available_at: DateTime<Utc>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
    pub(crate) idempotency_key: Option<String>,
    pub(crate) attempt_count: u32,
    pub(crate) max_attempts: u32,
    pub(crate) retry_initial_delay_ns: i64,
    pub(crate) retry_max_delay_ns: i64,
    pub(crate) cancel_requested_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) terminal_reason: Option<String>,
}

impl Task {
    /// The identifier of the task.
    pub fn id(&self) -> TaskId {
        self.id
    }

    /// The namespace of the task.
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// The queue of the task.
    pub fn queue(&self) -> &QueueName {
        &self.queue
    }

    /// The kind of the task.
    pub fn kind(&self) -> &TaskKind {
        &self.kind
    }

    /// The opaque payload of the task.
    pub fn payload(&self) -> &Value {
        &self.payload
    }

    /// The opaque result of the task, if it finished successfully.
    pub fn result(&self) -> Option<&Value> {
        self.result.as_ref()
    }

    /// The state of the task.
    pub fn status(&self) -> TaskStatus {
        self.status
    }

    /// The priority of the task. Higher priorities are claimed first.
    pub fn priority(&self) -> i32 {
        self.priority
    }

    /// The earliest time at which the task may be claimed.
    pub fn available_at(&self) -> DateTime<Utc> {
        self.available_at
    }

    /// The time at which the task was created.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// The time at which the task was changed the last time.
    pub fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at
    }

    /// The idempotency key of the task, if it has one.
    pub fn idempotency_key(&self) -> Option<&str> {
        self.idempotency_key.as_deref()
    }

    /// The number of attempts that were started for the task.
    pub fn attempt_count(&self) -> u32 {
        self.attempt_count
    }

    /// The number of attempts the task may reach.
    pub fn max_attempts(&self) -> u32 {
        self.max_attempts
    }

    /// The delay before the first retry of the task.
    pub fn retry_initial_delay(&self) -> Duration {
        Duration::from_nanos(self.retry_initial_delay_ns.max(0) as u64)
    }

    /// The upper bound of the retry delay of the task.
    pub fn retry_max_delay(&self) -> Duration {
        Duration::from_nanos(self.retry_max_delay_ns.max(0) as u64)
    }

    /// The time at which a cancellation was requested, if it was.
    pub fn cancel_requested_at(&self) -> Option<DateTime<Utc>> {
        self.cancel_requested_at
    }

    /// The time at which the task reached a terminal state, if it did.
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    /// The reason of the terminal state, if the task has one.
    pub fn terminal_reason(&self) -> Option<&str> {
        self.terminal_reason.as_deref()
    }

    /// Whether the task reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        self.status.is_terminal()
    }

    /// The cursor that continues the listing after this task.
    pub fn cursor(&self) -> ListCursor {
        ListCursor {
            created_at: self.created_at,
            id: self.id,
        }
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Task")
            .field("id", &self.id)
            .field("namespace", &self.namespace)
            .field("queue", &self.queue)
            .field("kind", &self.kind)
            .field("payload", &"<opaque>")
            .field("result", &self.result.as_ref().map(|_| "<opaque>"))
            .field("status", &self.status)
            .field("priority", &self.priority)
            .field("available_at", &self.available_at)
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .field("idempotency_key", &self.idempotency_key)
            .field("attempt_count", &self.attempt_count)
            .field("max_attempts", &self.max_attempts)
            .field("cancel_requested_at", &self.cancel_requested_at)
            .field("finished_at", &self.finished_at)
            .field("terminal_reason", &self.terminal_reason)
            .finish()
    }
}

/// An attempt of a task.
///
/// The error of an attempt belongs to the caller. It is opaque data that the
/// library never interprets, never logs and never prints.
#[derive(Clone, PartialEq)]
pub struct Attempt {
    pub(crate) id: AttemptId,
    pub(crate) task_id: TaskId,
    pub(crate) attempt_number: u32,
    pub(crate) worker_id: String,
    pub(crate) fencing_token: FencingToken,
    pub(crate) status: AttemptStatus,
    pub(crate) lease_expires_at: Option<DateTime<Utc>>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
    pub(crate) error: Option<Value>,
    pub(crate) progress: Option<ProgressUpdate>,
    pub(crate) progress_updated_at: Option<DateTime<Utc>>,
}

impl Attempt {
    /// The identifier of the attempt.
    pub fn id(&self) -> AttemptId {
        self.id
    }

    /// The task of the attempt.
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    /// The number of the attempt, starting at one.
    pub fn attempt_number(&self) -> u32 {
        self.attempt_number
    }

    /// The worker that owns the attempt.
    pub fn worker_id(&self) -> &str {
        &self.worker_id
    }

    /// The fencing token of the attempt. It is valid while the attempt runs.
    pub fn fencing_token(&self) -> FencingToken {
        self.fencing_token
    }

    /// The state of the attempt.
    pub fn status(&self) -> AttemptStatus {
        self.status
    }

    /// The time at which the lease of the attempt expires.
    pub fn lease_expires_at(&self) -> Option<DateTime<Utc>> {
        self.lease_expires_at
    }

    /// The time at which the attempt started.
    pub fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }

    /// The time at which the attempt was sealed, if it was.
    pub fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.finished_at
    }

    /// The opaque error of the attempt, if it carries one.
    pub fn error(&self) -> Option<&Value> {
        self.error.as_ref()
    }

    /// The last progress report of the attempt.
    pub fn progress(&self) -> Option<&ProgressUpdate> {
        self.progress.as_ref()
    }

    /// The time at which the last progress report was written.
    pub fn progress_updated_at(&self) -> Option<DateTime<Utc>> {
        self.progress_updated_at
    }
}

impl fmt::Debug for Attempt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Attempt")
            .field("id", &self.id)
            .field("task_id", &self.task_id)
            .field("attempt_number", &self.attempt_number)
            .field("worker_id", &self.worker_id)
            .field("fencing_token", &self.fencing_token)
            .field("status", &self.status)
            .field("lease_expires_at", &self.lease_expires_at)
            .field("started_at", &self.started_at)
            .field("finished_at", &self.finished_at)
            .field("error", &self.error.as_ref().map(|_| "<opaque>"))
            .field("progress", &self.progress)
            .field("progress_updated_at", &self.progress_updated_at)
            .finish()
    }
}

/// A task with the attempt that was claimed for it.
#[derive(Debug, Clone, PartialEq)]
pub struct Claim {
    task: Task,
    attempt: Attempt,
}

impl Claim {
    /// The claimed task.
    pub fn task(&self) -> &Task {
        &self.task
    }

    /// The attempt that holds the lease of the task.
    pub fn attempt(&self) -> &Attempt {
        &self.attempt
    }

    /// Splits the claim into its task and its attempt.
    pub fn into_parts(self) -> (Task, Attempt) {
        (self.task, self.attempt)
    }

    pub(crate) fn from_parts(task: Task, attempt: Attempt) -> Self {
        Self { task, attempt }
    }
}

/// The result of an enqueue.
#[derive(Debug, Clone, PartialEq)]
pub enum EnqueueOutcome {
    /// The task was created.
    Created(Task),
    /// A task with the same idempotency key and the same request already
    /// existed.
    Existing(Task),
}

impl EnqueueOutcome {
    /// The task of the outcome.
    pub fn task(&self) -> &Task {
        match self {
            Self::Created(task) | Self::Existing(task) => task,
        }
    }

    /// Whether the task was created by this call.
    pub fn is_created(&self) -> bool {
        matches!(self, Self::Created(_))
    }

    /// Unwraps the outcome into its task.
    pub fn into_task(self) -> Task {
        match self {
            Self::Created(task) | Self::Existing(task) => task,
        }
    }
}

/// The position of a task in a listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ListCursor {
    created_at: DateTime<Utc>,
    id: TaskId,
}

impl ListCursor {
    /// Builds a cursor for a position in a listing.
    pub fn new(created_at: DateTime<Utc>, id: TaskId) -> Self {
        Self { created_at, id }
    }

    /// The creation time of the task the cursor points at.
    pub fn created_at(&self) -> DateTime<Utc> {
        self.created_at
    }

    /// The task the cursor points at.
    pub fn id(&self) -> TaskId {
        self.id
    }
}

/// The outcome of a failed attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct Failure {
    error: Option<Value>,
    retryable: bool,
}

impl Failure {
    /// A failure that ends the task.
    pub fn permanent(error: Option<Value>) -> Self {
        Self {
            error,
            retryable: false,
        }
    }

    /// A failure that may be retried while the budget of the task allows it.
    pub fn retryable(error: Option<Value>) -> Self {
        Self {
            error,
            retryable: true,
        }
    }

    /// The opaque error of the failure.
    pub fn error(&self) -> Option<&Value> {
        self.error.as_ref()
    }

    /// Whether the failure may be retried.
    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_reject_empty_and_unsafe_characters() {
        assert!(Namespace::new("").is_err());
        assert!(Namespace::new("a b").is_err());
        assert!(Namespace::new("a/b").is_err());
        assert!(Namespace::new(&"a".repeat(MAX_NAME_LENGTH)).is_ok());
        assert!(Namespace::new(&"a".repeat(MAX_NAME_LENGTH + 1)).is_err());
        assert!(QueueName::new("queue:one").is_ok());
        assert!(TaskKind::new("stock.k8s:fetch").is_ok());
    }

    #[test]
    fn names_deserialize_through_validation() {
        let parsed: Result<Namespace, _> = serde_json::from_str("\"bad name\"");
        assert!(parsed.is_err());
        let parsed: Namespace = serde_json::from_str("\"good-name\"").unwrap();
        assert_eq!(parsed.as_str(), "good-name");
    }

    #[test]
    fn task_status_round_trips() {
        for status in [
            TaskStatus::Queued,
            TaskStatus::Running,
            TaskStatus::CancelRequested,
            TaskStatus::Succeeded,
            TaskStatus::Failed,
            TaskStatus::Cancelled,
        ] {
            assert_eq!(TaskStatus::from_db(status.as_str()).unwrap(), status);
        }
        assert!(TaskStatus::from_db("unknown").is_err());
        assert!(TaskStatus::Succeeded.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
    }

    #[test]
    fn retry_policy_validates_bounds() {
        assert!(RetryPolicy::new(0, Duration::ZERO, Duration::ZERO).is_err());
        assert!(RetryPolicy::new(2, Duration::from_secs(2), Duration::from_secs(1)).is_err());
        assert!(
            RetryPolicy::new(
                2,
                Duration::from_secs(1),
                MAX_RETRY_DELAY + Duration::from_secs(1)
            )
            .is_err()
        );
        assert!(RetryPolicy::new(3, Duration::from_secs(1), Duration::from_secs(60)).is_ok());
        assert_eq!(RetryPolicy::none().max_attempts(), 1);
    }

    #[test]
    fn canonical_spec_ignores_key_order_and_enqueue_time() {
        let kind = TaskKind::new("demo").unwrap();
        let first = EnqueueRequest::new(kind.clone(), serde_json::json!({"b": 2, "a": 1}))
            .with_priority(3)
            .with_retry_policy(
                RetryPolicy::new(2, Duration::from_secs(1), Duration::from_secs(5)).unwrap(),
            );
        let second = EnqueueRequest::new(kind, serde_json::json!({"a": 1, "b": 2}))
            .with_priority(3)
            .with_retry_policy(
                RetryPolicy::new(2, Duration::from_secs(1), Duration::from_secs(5)).unwrap(),
            );

        assert_eq!(
            canonical_request_spec(&first),
            canonical_request_spec(&second)
        );
        assert!(canonical_request_spec(&first).contains("\"available_at\":null"));
    }

    #[test]
    fn canonical_spec_separates_different_requests() {
        let kind = TaskKind::new("demo").unwrap();
        let base = EnqueueRequest::new(kind.clone(), serde_json::json!({"a": 1}));
        let other_payload = EnqueueRequest::new(kind.clone(), serde_json::json!({"a": 2}));
        let other_priority =
            EnqueueRequest::new(kind, serde_json::json!({"a": 1})).with_priority(1);

        assert_ne!(
            canonical_request_spec(&base),
            canonical_request_spec(&other_payload)
        );
        assert_ne!(
            canonical_request_spec(&base),
            canonical_request_spec(&other_priority)
        );
    }

    #[test]
    fn idempotency_key_is_validated() {
        let request = EnqueueRequest::new(TaskKind::new("demo").unwrap(), Value::Null);
        assert!(request.clone().with_idempotency_key("").is_err());
        assert!(request.clone().with_idempotency_key("a\nb").is_err());
        assert!(request.with_idempotency_key("key-1").is_ok());
    }

    #[test]
    fn progress_validates_bounds() {
        let progress = ProgressUpdate::new("import")
            .with_completed(3)
            .with_total(10);
        assert!(progress.validate().is_ok());
        assert!(
            progress
                .percent()
                .is_some_and(|value| (value - 30.0).abs() < f64::EPSILON)
        );

        let invalid = ProgressUpdate::new("import")
            .with_completed(11)
            .with_total(10);
        assert!(invalid.validate().is_err());

        let unknown_total = ProgressUpdate::new("import").with_completed(3);
        assert!(unknown_total.validate().is_ok());
        assert!(unknown_total.percent().is_none());

        let empty_total = ProgressUpdate::new("import").with_total(0);
        assert!(empty_total.percent().is_none());
    }

    #[test]
    fn retry_policy_round_trips_through_serde() {
        let policy = RetryPolicy::new(3, Duration::from_secs(1), Duration::from_secs(30)).unwrap();
        let encoded = serde_json::to_string(&policy).unwrap();
        let decoded: RetryPolicy = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, policy);
    }

    #[test]
    fn an_invalid_retry_policy_is_refused_by_serde() {
        let decoded: Result<RetryPolicy, _> =
            serde_json::from_str("{\"max_attempts\":0,\"initial_delay_ns\":0,\"max_delay_ns\":0}");

        assert!(decoded.is_err());
    }

    #[test]
    fn progress_round_trips_through_serde() {
        let progress = ProgressUpdate::new("import")
            .with_completed(3)
            .with_total(10)
            .with_unit("pages");
        let encoded = serde_json::to_string(&progress).unwrap();
        let decoded: ProgressUpdate = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, progress);
    }

    #[test]
    fn an_invalid_progress_report_is_refused_by_serde() {
        let decoded: Result<ProgressUpdate, _> = serde_json::from_str(
            "{\"stage\":\"import\",\"completed\":11,\"total\":10,\"unit\":null}",
        );

        assert!(decoded.is_err());
    }

    #[test]
    fn the_debug_output_of_a_task_hides_the_payload() {
        let task = Task {
            id: TaskId::new(),
            namespace: Namespace::new("ns").unwrap(),
            queue: QueueName::new("q").unwrap(),
            kind: TaskKind::new("demo").unwrap(),
            payload: serde_json::json!({"secret": "value"}),
            result: Some(serde_json::json!({"secret": "value"})),
            status: TaskStatus::Queued,
            priority: 0,
            available_at: DateTime::UNIX_EPOCH,
            created_at: DateTime::UNIX_EPOCH,
            updated_at: DateTime::UNIX_EPOCH,
            idempotency_key: None,
            attempt_count: 0,
            max_attempts: 1,
            retry_initial_delay_ns: 0,
            retry_max_delay_ns: 0,
            cancel_requested_at: None,
            finished_at: None,
            terminal_reason: None,
        };
        let attempt = Attempt {
            id: AttemptId::new(),
            task_id: task.id,
            attempt_number: 1,
            worker_id: "worker-1".to_owned(),
            fencing_token: FencingToken::new(),
            status: AttemptStatus::Running,
            lease_expires_at: Some(DateTime::UNIX_EPOCH),
            started_at: DateTime::UNIX_EPOCH,
            finished_at: None,
            error: Some(serde_json::json!({"secret": "value"})),
            progress: None,
            progress_updated_at: None,
        };

        let rendered = format!("{task:?} {attempt:?}");
        assert!(
            !rendered.contains("secret"),
            "the payload of a task must not be printed: {rendered}"
        );
    }
}
