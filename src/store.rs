//! The durable store of tasks and attempts.
//!
//! Every statement of the store lives in `sql/store_*.sql` and is loaded with
//! `query_file!` or `query_file_as!`. The store never creates or alters the
//! schema and never writes outside the schema of the library.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::PgConnection;
use std::time::Duration;
use uuid::Uuid;

use crate::error::{Error, Result};
use crate::migrate::{OwnershipRow, SCHEMA_NAME};
use crate::model::{
    Attempt, AttemptId, AttemptStatus, Failure, FencingToken, ListCursor, Namespace,
    ProgressUpdate, QueueName, Task, TaskId, TaskKind, TaskStatus,
};

#[derive(sqlx::FromRow)]
pub(crate) struct TaskRow {
    id: Uuid,
    namespace: String,
    queue: String,
    kind: String,
    payload: Value,
    result: Option<Value>,
    status: String,
    priority: i32,
    available_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    idempotency_key: Option<String>,
    // The specification is part of the row, the store never reads it.
    #[allow(dead_code)]
    request_spec: Value,
    attempt_count: i32,
    max_attempts: i32,
    retry_initial_delay_ns: i64,
    retry_max_delay_ns: i64,
    cancel_requested_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    terminal_reason: Option<String>,
}

#[derive(sqlx::FromRow)]
struct LockRow {
    task_found: Option<i64>,
    attempt_found: Option<i64>,
}

#[derive(sqlx::FromRow)]
pub(crate) struct AttemptRow {
    id: Uuid,
    task_id: Uuid,
    attempt_number: i32,
    worker_id: String,
    fencing_token: Uuid,
    status: String,
    lease_expires_at: Option<DateTime<Utc>>,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    error: Option<Value>,
    progress: Option<Value>,
    progress_updated_at: Option<DateTime<Utc>>,
}

/// Checks that a connection belongs to the schema instance of the queue.
///
/// A transaction of another database is refused, also when that database holds
/// a schema of the library.
pub(crate) async fn verify_instance(connection: &mut PgConnection, expected: Uuid) -> Result<()> {
    let present = sqlx::query_file_scalar!("sql/store_marker_present.sql")
        .fetch_one(&mut *connection)
        .await?;
    if !present.unwrap_or(false) {
        return Err(Error::SchemaMissing {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let marker = sqlx::query_file_as!(OwnershipRow, "sql/migration_ownership.sql")
        .fetch_optional(&mut *connection)
        .await?
        .ok_or(Error::SchemaMissing {
            schema: SCHEMA_NAME.to_owned(),
        })?;
    if marker.instance_uuid != expected {
        return Err(Error::InstanceMismatch {
            expected,
            found: marker.instance_uuid,
        });
    }
    Ok(())
}

/// The largest number of additional attempts a manual retry can grant.
///
/// The budget of a task is stored in a column of a signed 32 bit integer, the
/// limit keeps the sum of the attempts that ran and the additional attempts
/// inside that range.
pub(crate) const MAX_ADDITIONAL_ATTEMPTS: u32 = i32::MAX as u32;

/// The error of an operation whose lease guard did not match.
pub(crate) async fn lease_failure(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Error {
    let exists = sqlx::query_file_scalar!(
        "sql/store_task_exists.sql",
        namespace.as_str(),
        queue.as_str(),
        task_id.as_uuid(),
    )
    .fetch_one(&mut *connection)
    .await
    .unwrap_or(None)
    .unwrap_or(false);
    if exists {
        Error::LeaseLost {
            task_id: task_id.as_uuid(),
        }
    } else {
        Error::NotFound
    }
}

/// Locks the attempt of a lease guarded operation.
///
/// The lock is taken before the lease of the attempt is checked, so that the
/// check uses the clock of the database after a lock wait. The caller keeps
/// the lock until the end of its transaction.
pub(crate) async fn lock_attempt(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
) -> Result<()> {
    let row = sqlx::query_file_as!(
        LockRow,
        "sql/store_lock_attempt.sql",
        namespace.as_str(),
        queue.as_str(),
        attempt.task_id().as_uuid(),
        attempt.id().as_uuid(),
        attempt.fencing_token().as_uuid(),
    )
    .fetch_one(&mut *connection)
    .await?;
    match (
        row.task_found.unwrap_or_default(),
        row.attempt_found.unwrap_or_default(),
    ) {
        (0, _) => Err(Error::NotFound),
        (_, 0) => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
        _ => Ok(()),
    }
}

/// Locks the task and the attempt of a lease guarded operation.
///
/// The task is locked before the attempt, so that all operations of a task
/// take its locks in the same order. The caller keeps the locks until the end
/// of its transaction.
pub(crate) async fn lock_task_attempt(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
) -> Result<()> {
    let row = sqlx::query_file_as!(
        LockRow,
        "sql/store_lock_task_attempt.sql",
        namespace.as_str(),
        queue.as_str(),
        attempt.task_id().as_uuid(),
        attempt.id().as_uuid(),
        attempt.fencing_token().as_uuid(),
    )
    .fetch_one(&mut *connection)
    .await?;
    match (
        row.task_found.unwrap_or_default(),
        row.attempt_found.unwrap_or_default(),
    ) {
        (0, _) => Err(Error::NotFound),
        (_, 0) => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
        _ => Ok(()),
    }
}

/// Notifies the workers that a task is ready.
pub(crate) async fn notify_ready(connection: &mut PgConnection) -> Result<()> {
    sqlx::query_file!("sql/store_notify_ready.sql")
        .fetch_one(&mut *connection)
        .await?;
    Ok(())
}

/// Reads one task of the queue.
pub(crate) async fn task_by_id(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Option<Task>> {
    let row = sqlx::query_file_as!(
        TaskRow,
        "sql/store_task_by_id.sql",
        namespace.as_str(),
        queue.as_str(),
        task_id.as_uuid(),
    )
    .fetch_optional(&mut *connection)
    .await?;
    row.map(task_from_row).transpose()
}

/// Lists the tasks of the queue, newest first.
pub(crate) async fn list_tasks(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    cursor: Option<ListCursor>,
    limit: u32,
) -> Result<Vec<Task>> {
    let (created_at, id) = match cursor {
        Some(cursor) => (cursor.created_at(), cursor.id().as_uuid()),
        None => first_page_cursor(),
    };
    let rows = sqlx::query_file_as!(
        TaskRow,
        "sql/store_list_tasks.sql",
        namespace.as_str(),
        queue.as_str(),
        created_at,
        id,
        i64::from(limit),
    )
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(task_from_row).collect()
}

/// Lists the attempts of a task, newest first.
pub(crate) async fn list_attempts(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
    limit: u32,
) -> Result<Vec<Attempt>> {
    let rows = sqlx::query_file_as!(
        AttemptRow,
        "sql/store_list_attempts.sql",
        namespace.as_str(),
        queue.as_str(),
        task_id.as_uuid(),
        i64::from(limit),
    )
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(attempt_from_row).collect()
}

/// Claims the next ready task of the queue.
///
/// A target restricts the claim to one task of the queue. The claim is `None`
/// when the target is not ready, not of this queue or locked by another
/// transaction, never another task of the queue.
pub(crate) async fn claim_task(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    worker_id: &str,
    lease_ttl: Duration,
    target: Option<TaskId>,
) -> Result<Option<crate::model::Claim>> {
    let row = sqlx::query_file_as!(
        AttemptRow,
        "sql/store_claim.sql",
        namespace.as_str(),
        queue.as_str(),
        worker_id,
        Uuid::new_v4(),
        Uuid::new_v4(),
        duration_millis(lease_ttl),
        target.map(|task_id| task_id.as_uuid()),
    )
    .fetch_optional(&mut *connection)
    .await?;
    let attempt = match row {
        Some(row) => attempt_from_row(row)?,
        None => return Ok(None),
    };
    let task = task_by_id(connection, namespace, queue, attempt.task_id())
        .await?
        .ok_or(Error::NotFound)?;
    Ok(Some(crate::model::Claim::from_parts(task, attempt)))
}

/// Renews the lease of an attempt.
pub(crate) async fn renew_attempt(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    lease_ttl: Duration,
) -> Result<DateTime<Utc>> {
    lock_attempt(connection, namespace, queue, attempt).await?;
    let row = sqlx::query_file!(
        "sql/store_renew.sql",
        namespace.as_str(),
        queue.as_str(),
        attempt.task_id().as_uuid(),
        attempt.id().as_uuid(),
        attempt.fencing_token().as_uuid(),
        duration_millis(lease_ttl),
    )
    .fetch_optional(&mut *connection)
    .await?;
    match row {
        Some(row) => row.lease_expires_at.ok_or(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Completes an attempt and the task it belongs to.
pub(crate) async fn complete_attempt(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    result: Option<Value>,
) -> Result<Task> {
    lock_task_attempt(connection, namespace, queue, attempt).await?;
    let row = sqlx::query_file_as!(
        TaskRow,
        "sql/store_complete.sql",
        namespace.as_str(),
        queue.as_str(),
        attempt.task_id().as_uuid(),
        attempt.id().as_uuid(),
        attempt.fencing_token().as_uuid(),
        result,
    )
    .fetch_optional(&mut *connection)
    .await?;
    match row {
        Some(row) => task_from_row(row),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Fails an attempt. A retryable failure requeues the task while its budget
/// allows it.
pub(crate) async fn fail_attempt(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    failure: &Failure,
    retry_delay: Duration,
) -> Result<Task> {
    lock_task_attempt(connection, namespace, queue, attempt).await?;
    let row = sqlx::query_file_as!(
        TaskRow,
        "sql/store_fail.sql",
        namespace.as_str(),
        queue.as_str(),
        attempt.task_id().as_uuid(),
        attempt.id().as_uuid(),
        attempt.fencing_token().as_uuid(),
        failure.error().cloned(),
        failure.is_retryable(),
        duration_micros(retry_delay),
    )
    .fetch_optional(&mut *connection)
    .await?;
    match row {
        Some(row) => task_from_row(row),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Requests the cancellation of a task.
///
/// A queued task is cancelled at once, a running task is marked for
/// cancellation and a terminal task is reported unchanged.
pub(crate) async fn cancel_task(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Task> {
    let row = sqlx::query_file_as!(
        TaskRow,
        "sql/store_cancel.sql",
        namespace.as_str(),
        queue.as_str(),
        task_id.as_uuid(),
    )
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(row) = row {
        return task_from_row(row);
    }
    match task_by_id(connection, namespace, queue, task_id).await? {
        Some(task) => Ok(task),
        None => Err(Error::NotFound),
    }
}

/// Schedules a failed task again with a larger budget.
pub(crate) async fn retry_failed_task(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
    additional_attempts: u32,
) -> Result<Task> {
    let additional = i32::try_from(additional_attempts).map_err(|_| Error::Validation {
        field: "additional_attempts",
        reason: format!("must not exceed {MAX_ADDITIONAL_ATTEMPTS}"),
    })?;
    let row = sqlx::query_file_as!(
        TaskRow,
        "sql/store_retry_failed.sql",
        namespace.as_str(),
        queue.as_str(),
        task_id.as_uuid(),
        i64::from(additional),
        i64::from(i32::MAX),
    )
    .fetch_optional(&mut *connection)
    .await?;
    if let Some(row) = row {
        return task_from_row(row);
    }
    match task_by_id(connection, namespace, queue, task_id).await? {
        Some(task) => Err(Error::InvalidTaskState {
            task_id: task_id.as_uuid(),
            status: task.status().to_string(),
            action: "retry",
        }),
        None => Err(Error::NotFound),
    }
}

/// Seals the attempts whose lease expired and schedules their tasks again.
pub(crate) async fn recover_expired_tasks(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    limit: u32,
) -> Result<Vec<Task>> {
    let rows = sqlx::query_file_as!(
        TaskRow,
        "sql/store_recover_expired.sql",
        namespace.as_str(),
        queue.as_str(),
        i64::from(limit),
    )
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(task_from_row).collect()
}

fn task_from_row(row: TaskRow) -> Result<Task> {
    Ok(Task {
        id: TaskId::from_uuid(row.id),
        namespace: Namespace::new(&row.namespace)?,
        queue: QueueName::new(&row.queue)?,
        kind: TaskKind::new(&row.kind)?,
        payload: row.payload,
        result: row.result,
        status: TaskStatus::from_db(&row.status)?,
        priority: row.priority,
        available_at: row.available_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        idempotency_key: row.idempotency_key,
        attempt_count: u32::try_from(row.attempt_count).unwrap_or_default(),
        max_attempts: u32::try_from(row.max_attempts).unwrap_or_default(),
        retry_initial_delay_ns: row.retry_initial_delay_ns,
        retry_max_delay_ns: row.retry_max_delay_ns,
        cancel_requested_at: row.cancel_requested_at,
        finished_at: row.finished_at,
        terminal_reason: row.terminal_reason,
    })
}

fn attempt_from_row(row: AttemptRow) -> Result<Attempt> {
    Ok(Attempt {
        id: AttemptId::from_uuid(row.id),
        task_id: TaskId::from_uuid(row.task_id),
        attempt_number: u32::try_from(row.attempt_number).unwrap_or_default(),
        worker_id: row.worker_id,
        fencing_token: FencingToken::from_uuid(row.fencing_token),
        status: AttemptStatus::from_db(&row.status)?,
        lease_expires_at: row.lease_expires_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        error: row.error,
        progress: row.progress.map(progress_from_json).transpose()?,
        progress_updated_at: row.progress_updated_at,
    })
}

fn progress_from_json(value: Value) -> Result<ProgressUpdate> {
    serde_json::from_value(value).map_err(|error| Error::Validation {
        field: "progress",
        reason: format!("the stored progress report is invalid: {error}"),
    })
}

fn first_page_cursor() -> (DateTime<Utc>, Uuid) {
    let end_of_time = DateTime::UNIX_EPOCH + Duration::from_secs(253_402_300_799);
    (end_of_time, Uuid::from_u128(u128::MAX))
}

pub(crate) fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis())
        .unwrap_or(i64::MAX)
        .max(1)
}

pub(crate) fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros())
        .unwrap_or(i64::MAX)
        .max(1)
}
