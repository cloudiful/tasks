//! The durable store of tasks and attempts.
//!
//! Ordinary reads use the native entities of [`crate::entity`]. The guarded
//! state transitions, advisory locks and the PostgreSQL introspection keep the
//! statements in `sql/`, which are executed as raw statements with positional
//! values. The store never creates or alters the schema and never writes
//! outside the schema of the library.

use std::time::Duration;

use sea_orm::sea_query::{ArrayType, Value as SeaValue};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, EntityTrait, FromQueryResult, QueryFilter, QueryOrder,
    QueryResult, QuerySelect, Statement, TryGetable,
};
use serde_json::Value;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::entity::{
    attempt as attempt_entity, queue_policy as policy_entity, schema_ownership, task as task_entity,
};
use crate::error::{Error, Result};
use crate::migrate::SCHEMA_NAME;
use crate::model::{
    Attempt, AttemptId, AttemptStatus, Failure, FencingToken, ListCursor, Namespace,
    ProgressUpdate, QueueName, Task, TaskId, TaskKind, TaskPauseState, TaskStatus,
};
use crate::queue::QueuePolicy;
use crate::sql;

const MARKER_PRESENT: &str = include_str!("../sql/store_marker_present.sql");
const LOCK_QUEUE: &str = include_str!("../sql/store_lock_queue.sql");
const QUEUE_POLICY_SET: &str = include_str!("../sql/store_queue_policy_set.sql");
const QUEUE_POLICY_COUNT: &str = include_str!("../sql/store_queue_policy_count.sql");
const TRANSACTION_ISOLATION: &str = include_str!("../sql/store_queue_policy_isolation.sql");
const ATTEMPT_SEALED: &str = include_str!("../sql/store_attempt_sealed.sql");
const TASK_EXISTS: &str = include_str!("../sql/store_task_exists.sql");
const LOCK_ATTEMPT: &str = include_str!("../sql/store_lock_attempt.sql");
const LOCK_TASK: &str = include_str!("../sql/store_lock_task.sql");
const LOCK_TASK_IDS: &str = include_str!("../sql/store_lock_task_ids.sql");
const LOCK_TASK_ATTEMPT: &str = include_str!("../sql/store_lock_task_attempt.sql");
const NOTIFY_READY: &str = include_str!("../sql/store_notify_ready.sql");
const GET_TASKS_BY_IDS: &str = include_str!("../sql/store_get_tasks_by_ids.sql");
const HAS_LIVE_ATTEMPTS: &str = include_str!("../sql/store_has_live_attempts.sql");
const ACTIVE_TASKS_OF_KIND: &str = include_str!("../sql/store_active_tasks_of_kind.sql");
const ACTIVE_TASK_OF_KIND: &str = include_str!("../sql/store_active_task_of_kind.sql");
const CLAIM: &str = include_str!("../sql/store_claim.sql");
const RENEW: &str = include_str!("../sql/store_renew.sql");
const COMPLETE: &str = include_str!("../sql/store_complete.sql");
const FAIL: &str = include_str!("../sql/store_fail.sql");
const CANCEL: &str = include_str!("../sql/store_cancel.sql");
const RETRY_FAILED: &str = include_str!("../sql/store_retry_failed.sql");
const PAUSE_TASK: &str = include_str!("../sql/store_pause_task.sql");
const RESUME_TASK: &str = include_str!("../sql/store_resume_task.sql");
const PARK_PAUSED: &str = include_str!("../sql/store_park_paused.sql");
const RECOVER_EXPIRED: &str = include_str!("../sql/store_recover_expired.sql");

/// Checks that a connection belongs to the schema instance of the queue.
///
/// A transaction of another database is refused, also when that database holds
/// a schema of the library.
pub(crate) async fn verify_instance<C: ConnectionTrait>(
    connection: &C,
    expected: Uuid,
) -> Result<()> {
    let present = fetch_scalar::<bool>(connection, sql::statement(MARKER_PRESENT, vec![]))
        .await?
        .unwrap_or(false);
    if !present {
        return Err(Error::SchemaMissing {
            schema: SCHEMA_NAME.to_owned(),
        });
    }
    let marker = schema_ownership::Entity::find()
        .one(connection)
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

/// Takes the queue scoped transaction advisory lock.
///
/// Claims and policy replacement use the same lock, so a policy change and a
/// count-and-claim cannot interleave. The lock is held until the end of the
/// transaction of the caller.
pub(crate) async fn lock_queue<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
) -> Result<()> {
    connection
        .query_one_raw(sql::statement(
            LOCK_QUEUE,
            vec![namespace.as_str().into(), queue.as_str().into()],
        ))
        .await?;
    Ok(())
}

/// Reads the durable execution policy of the queue.
pub(crate) async fn queue_policy<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
) -> Result<QueuePolicy> {
    let row = policy_entity::Entity::find_by_id((
        namespace.as_str().to_owned(),
        queue.as_str().to_owned(),
    ))
    .one(connection)
    .await?;
    match row {
        Some(row) => {
            let limit = u32::try_from(row.max_concurrent_attempts)
                .ok()
                .filter(|limit| *limit > 0)
                .ok_or(Error::Validation {
                    field: "max_concurrent_attempts",
                    reason: "the stored policy is not a positive supported integer".to_owned(),
                })?;
            Ok(QueuePolicy {
                max_concurrent_attempts: Some(limit),
            })
        }
        None => Ok(QueuePolicy::unlimited()),
    }
}

/// Stores the positive concurrency limit of the queue.
pub(crate) async fn set_queue_policy<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    max_concurrent_attempts: i32,
) -> Result<()> {
    connection
        .execute_raw(sql::statement(
            QUEUE_POLICY_SET,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                max_concurrent_attempts.into(),
            ],
        ))
        .await?;
    Ok(())
}

/// Removes the stored policy of the queue and restores the unlimited default.
pub(crate) async fn clear_queue_policy<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
) -> Result<()> {
    policy_entity::Entity::delete_by_id((namespace.as_str().to_owned(), queue.as_str().to_owned()))
        .exec(connection)
        .await?;
    Ok(())
}

async fn running_attempt_count<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
) -> Result<i64> {
    Ok(fetch_scalar::<i64>(
        connection,
        sql::statement(
            QUEUE_POLICY_COUNT,
            vec![namespace.as_str().into(), queue.as_str().into()],
        ),
    )
    .await?
    .unwrap_or_default())
}

async fn transaction_isolation<C: ConnectionTrait>(connection: &C) -> Result<String> {
    fetch_scalar::<String>(connection, sql::statement(TRANSACTION_ISOLATION, vec![]))
        .await?
        .ok_or(Error::Validation {
            field: "transaction_isolation",
            reason: "the database did not report its isolation level".to_owned(),
        })
}

/// Whether the captured attempt already holds a terminal seal.
pub(crate) async fn attempt_is_sealed<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
) -> Result<bool> {
    Ok(fetch_scalar::<bool>(
        connection,
        sql::statement(
            ATTEMPT_SEALED,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
            ],
        ),
    )
    .await?
    .unwrap_or(false))
}

/// The largest number of additional attempts a manual retry can grant.
///
/// The budget of a task is stored in a column of a signed 32 bit integer, the
/// limit keeps the sum of the attempts that ran and the additional attempts
/// inside that range.
pub(crate) const MAX_ADDITIONAL_ATTEMPTS: u32 = i32::MAX as u32;

/// The error of an operation whose lease guard did not match.
pub(crate) async fn lease_failure<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Error {
    let exists = fetch_scalar::<bool>(
        connection,
        sql::statement(
            TASK_EXISTS,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                task_id.as_uuid().into(),
            ],
        ),
    )
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
pub(crate) async fn lock_attempt<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
) -> Result<()> {
    let row = connection
        .query_one_raw(sql::statement(
            LOCK_ATTEMPT,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
            ],
        ))
        .await?
        .ok_or_else(|| Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        })?;
    lock_outcome(&row, attempt)
}

/// Locks the task and the attempt of a lease guarded operation.
///
/// The task is locked before the attempt, so that all operations of a task
/// take its locks in the same order. The caller keeps the locks until the end
/// of its transaction.
pub(crate) async fn lock_task_attempt<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
) -> Result<()> {
    let row = connection
        .query_one_raw(sql::statement(
            LOCK_TASK_ATTEMPT,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
            ],
        ))
        .await?
        .ok_or_else(|| Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        })?;
    lock_outcome(&row, attempt)
}

fn lock_outcome(row: &QueryResult, attempt: &Attempt) -> Result<()> {
    let task_found = row.try_get_by::<Option<i64>, usize>(0)?.unwrap_or_default();
    let attempt_found = row.try_get_by::<Option<i64>, usize>(1)?.unwrap_or_default();
    match (task_found, attempt_found) {
        (0, _) => Err(Error::NotFound),
        (_, 0) => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
        _ => Ok(()),
    }
}

/// Notifies the workers that a task is ready.
pub(crate) async fn notify_ready<C: ConnectionTrait>(connection: &C) -> Result<()> {
    connection
        .query_one_raw(sql::statement(NOTIFY_READY, vec![]))
        .await?;
    Ok(())
}

/// Reads one task of the queue.
pub(crate) async fn task_by_id<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Option<Task>> {
    let row = task_entity::Entity::find_by_id(task_id.as_uuid())
        .filter(task_entity::Column::Namespace.eq(namespace.as_str()))
        .filter(task_entity::Column::Queue.eq(queue.as_str()))
        .one(connection)
        .await?;
    row.map(task_from_model).transpose()
}

/// Reads one task of the queue by its idempotency key.
///
/// The read is scoped to the namespace and the queue of the caller. A missing
/// key, or a key of another namespace or queue, yields `None`.
pub(crate) async fn task_by_idempotency_key<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    idempotency_key: &str,
) -> Result<Option<Task>> {
    let row = task_entity::Entity::find()
        .filter(task_entity::Column::Namespace.eq(namespace.as_str()))
        .filter(task_entity::Column::Queue.eq(queue.as_str()))
        .filter(task_entity::Column::IdempotencyKey.eq(idempotency_key))
        .one(connection)
        .await?;
    row.map(task_from_model).transpose()
}

/// Locks one task of the queue for update.
pub(crate) async fn lock_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Task> {
    let row = connection
        .query_one_raw(sql::statement(
            LOCK_TASK,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                task_id.as_uuid().into(),
            ],
        ))
        .await?
        .ok_or(Error::NotFound)?;
    task_from_model(task_entity::Model::from_query_result(&row, "")?)
}

/// Reads the present tasks of the queue from a batch of identifiers.
pub(crate) async fn tasks_by_ids<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_ids: &[Uuid],
) -> Result<Vec<Task>> {
    let rows = connection
        .query_all_raw(sql::statement(
            GET_TASKS_BY_IDS,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                uuid_array(task_ids),
            ],
        ))
        .await?;
    rows.into_iter()
        .map(|row| task_from_model(task_entity::Model::from_query_result(&row, "")?))
        .collect()
}

/// Reports whether any task of a batch holds a running attempt with a live
/// lease.
///
/// Every task of the batch is locked for share before the attempts are read.
/// A task that is absent or out of scope is refused instead of reported as
/// without a live attempt.
pub(crate) async fn has_live_attempts<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_ids: &[Uuid],
) -> Result<bool> {
    let locked = connection
        .query_all_raw(sql::statement(
            LOCK_TASK_IDS,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                uuid_array(task_ids),
            ],
        ))
        .await?;
    if locked.len() != task_ids.len() {
        return Err(Error::NotFound);
    }
    Ok(fetch_scalar::<bool>(
        connection,
        sql::statement(
            HAS_LIVE_ATTEMPTS,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                uuid_array(task_ids),
            ],
        ),
    )
    .await?
    .unwrap_or(false))
}

/// Reads the active tasks of one kind in the transaction of the caller.
///
/// A task is active when its status is queued, running or cancel requested, or
/// when it holds a running attempt whose lease expires within the grace of
/// the caller. The lease is compared against the transaction clock. The read
/// takes no row lock.
///
/// More than [`crate::queue::MAX_TASK_BATCH_SIZE`] matching tasks are refused
/// instead of truncated, so a caller never reads a partial activity result.
pub(crate) async fn active_tasks_of_kind<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    kind: &TaskKind,
    lease_grace: Duration,
) -> Result<Vec<Task>> {
    let rows = connection
        .query_all_raw(sql::statement(
            ACTIVE_TASKS_OF_KIND,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                kind.as_ref().into(),
                duration_micros(lease_grace).into(),
            ],
        ))
        .await?;
    if rows.len() > crate::queue::MAX_TASK_BATCH_SIZE {
        return Err(Error::Validation {
            field: "activity",
            reason: format!(
                "activity result exceeds the bound of {} tasks",
                crate::queue::MAX_TASK_BATCH_SIZE
            ),
        });
    }
    rows.into_iter()
        .map(|row| task_from_model(task_entity::Model::from_query_result(&row, "")?))
        .collect()
}

/// Reads one active task of one kind in the transaction of the caller.
///
/// A task that is absent or belongs to another namespace, another queue or
/// another kind is reported as `NotFound`. A task of the scope that is not
/// active is reported as `None`. The read takes no row lock.
pub(crate) async fn active_task_of_kind<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    kind: &TaskKind,
    task_id: TaskId,
    lease_grace: Duration,
) -> Result<Option<Task>> {
    let task = task_by_id(connection, namespace, queue, task_id)
        .await?
        .filter(|task| task.kind() == kind)
        .ok_or(Error::NotFound)?;
    match task.status() {
        TaskStatus::Queued | TaskStatus::Running | TaskStatus::CancelRequested => Ok(Some(task)),
        _ => {
            let live = fetch_scalar::<bool>(
                connection,
                sql::statement(
                    ACTIVE_TASK_OF_KIND,
                    vec![
                        task_id.as_uuid().into(),
                        duration_micros(lease_grace).into(),
                    ],
                ),
            )
            .await?
            .unwrap_or(false);
            Ok(live.then_some(task))
        }
    }
}

/// Lists the tasks of the queue, newest first.
pub(crate) async fn list_tasks<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    cursor: Option<ListCursor>,
    limit: u32,
) -> Result<Vec<Task>> {
    let mut query = task_entity::Entity::find()
        .filter(task_entity::Column::Namespace.eq(namespace.as_str()))
        .filter(task_entity::Column::Queue.eq(queue.as_str()));
    let (created_at, id) = match cursor {
        Some(cursor) => (cursor.utc_created_at()?, cursor.id().as_uuid()),
        None => first_page_cursor(),
    };
    query = query.filter(
        Condition::any()
            .add(task_entity::Column::CreatedAt.lt(created_at))
            .add(
                Condition::all()
                    .add(task_entity::Column::CreatedAt.eq(created_at))
                    .add(task_entity::Column::Id.lt(id)),
            ),
    );
    let rows = query
        .order_by_desc(task_entity::Column::CreatedAt)
        .order_by_desc(task_entity::Column::Id)
        .limit(u64::from(limit))
        .all(connection)
        .await?;
    rows.into_iter().map(task_from_model).collect()
}

/// Lists the attempts of a task, newest first.
pub(crate) async fn list_attempts<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
    limit: u32,
) -> Result<Vec<Attempt>> {
    let scoped = task_entity::Entity::find_by_id(task_id.as_uuid())
        .filter(task_entity::Column::Namespace.eq(namespace.as_str()))
        .filter(task_entity::Column::Queue.eq(queue.as_str()))
        .one(connection)
        .await?
        .is_some();
    if !scoped {
        return Ok(Vec::new());
    }
    let rows = attempt_entity::Entity::find()
        .filter(attempt_entity::Column::TaskId.eq(task_id.as_uuid()))
        .order_by_desc(attempt_entity::Column::AttemptNumber)
        .limit(u64::from(limit))
        .all(connection)
        .await?;
    rows.into_iter().map(attempt_from_model).collect()
}

/// Claims the next ready task of the queue.
///
/// A target restricts the claim to one task of the queue. The claim is `None`
/// when the target is not ready, not of this queue or locked by another
/// transaction, never another task of the queue.
pub(crate) async fn claim_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    worker_id: &str,
    lease_ttl: Duration,
    target: Option<TaskId>,
) -> Result<Option<crate::model::Claim>> {
    // The lock statement runs on its own so that a READ COMMITTED transaction
    // takes a fresh snapshot after it acquired the lock. A limit that was
    // stored while the claim waited is therefore visible to the count below.
    lock_queue(connection, namespace, queue).await?;
    // Every claim requires READ COMMITTED, whether or not the queue has a
    // policy now. An older snapshot could otherwise miss a policy committed
    // while the claim waited, or act on a stale policy and running count.
    if transaction_isolation(connection).await? != "read committed" {
        return Err(Error::Validation {
            field: "transaction_isolation",
            reason: "all task claims require READ COMMITTED".to_owned(),
        });
    }
    if let Some(limit) = queue_policy(connection, namespace, queue)
        .await?
        .max_concurrent_attempts
    {
        let running = running_attempt_count(connection, namespace, queue).await?;
        if running >= i64::from(limit) {
            return Ok(None);
        }
    }

    let row = connection
        .query_one_raw(sql::statement(
            CLAIM,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                worker_id.into(),
                Uuid::new_v4().into(),
                Uuid::new_v4().into(),
                duration_millis(lease_ttl).into(),
                target.map(|task_id| task_id.as_uuid()).into(),
            ],
        ))
        .await?;
    let attempt = match row {
        Some(row) => attempt_from_model(attempt_entity::Model::from_query_result(&row, "")?)?,
        None => return Ok(None),
    };
    let task = task_by_id(connection, namespace, queue, attempt.task_id())
        .await?
        .ok_or(Error::NotFound)?;
    Ok(Some(crate::model::Claim::from_parts(task, attempt)))
}

/// Renews the lease of an attempt.
pub(crate) async fn renew_attempt<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    lease_ttl: Duration,
) -> Result<OffsetDateTime> {
    lock_attempt(connection, namespace, queue, attempt).await?;
    let row = connection
        .query_one_raw(sql::statement(
            RENEW,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
                duration_millis(lease_ttl).into(),
            ],
        ))
        .await?;
    match row {
        Some(row) => row
            .try_get_by::<Option<OffsetDateTime>, usize>(0)?
            .ok_or(Error::LeaseLost {
                task_id: attempt.task_id().as_uuid(),
            }),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Completes an attempt and the task it belongs to.
pub(crate) async fn complete_attempt<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    result: Option<Value>,
) -> Result<Task> {
    lock_task_attempt(connection, namespace, queue, attempt).await?;
    let row = connection
        .query_one_raw(sql::statement(
            COMPLETE,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
                result.into(),
            ],
        ))
        .await?;
    match row {
        Some(row) => task_from_model(task_entity::Model::from_query_result(&row, "")?),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Fails an attempt. A retryable failure requeues the task while its budget
/// allows it.
pub(crate) async fn fail_attempt<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    failure: &Failure,
    retry_delay: Duration,
) -> Result<Task> {
    lock_task_attempt(connection, namespace, queue, attempt).await?;
    let row = connection
        .query_one_raw(sql::statement(
            FAIL,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
                failure.error().cloned().into(),
                failure.is_retryable().into(),
                duration_micros(retry_delay).into(),
            ],
        ))
        .await?;
    match row {
        Some(row) => task_from_model(task_entity::Model::from_query_result(&row, "")?),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Requests the cancellation of a task.
///
/// A queued task is cancelled at once, a running task is marked for
/// cancellation and a terminal task is reported unchanged.
pub(crate) async fn cancel_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Task> {
    let row = connection
        .query_one_raw(sql::statement(
            CANCEL,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                task_id.as_uuid().into(),
            ],
        ))
        .await?;
    if let Some(row) = row {
        return task_from_model(task_entity::Model::from_query_result(&row, "")?);
    }
    match task_by_id(connection, namespace, queue, task_id).await? {
        Some(task) => Ok(task),
        None => Err(Error::NotFound),
    }
}

/// Schedules a failed task again with a larger budget.
pub(crate) async fn retry_failed_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
    additional_attempts: u32,
) -> Result<Task> {
    let additional = i32::try_from(additional_attempts).map_err(|_| Error::Validation {
        field: "additional_attempts",
        reason: format!("must not exceed {MAX_ADDITIONAL_ATTEMPTS}"),
    })?;
    let row = connection
        .query_one_raw(sql::statement(
            RETRY_FAILED,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                task_id.as_uuid().into(),
                i64::from(additional).into(),
                i64::from(i32::MAX).into(),
            ],
        ))
        .await?;
    if let Some(row) = row {
        return task_from_model(task_entity::Model::from_query_result(&row, "")?);
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

/// Requests the cooperative pause of one task.
pub(crate) async fn pause_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Task> {
    let row = connection
        .query_one_raw(sql::statement(
            PAUSE_TASK,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                task_id.as_uuid().into(),
            ],
        ))
        .await?;
    if let Some(row) = row {
        return task_from_model(task_entity::Model::from_query_result(&row, "")?);
    }
    match task_by_id(connection, namespace, queue, task_id).await? {
        Some(task) if matches!(task.status(), TaskStatus::Queued | TaskStatus::Running) => Ok(task),
        Some(task) => Err(Error::InvalidTaskState {
            task_id: task_id.as_uuid(),
            status: task.status().to_string(),
            action: "paused",
        }),
        None => Err(Error::NotFound),
    }
}

/// Resumes a pausing or paused task.
pub(crate) async fn resume_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Task> {
    let row = connection
        .query_one_raw(sql::statement(
            RESUME_TASK,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                task_id.as_uuid().into(),
            ],
        ))
        .await?;
    if let Some(row) = row {
        return task_from_model(task_entity::Model::from_query_result(&row, "")?);
    }
    match task_by_id(connection, namespace, queue, task_id).await? {
        Some(task) if matches!(task.status(), TaskStatus::Queued | TaskStatus::Running) => Ok(task),
        Some(task) => Err(Error::InvalidTaskState {
            task_id: task_id.as_uuid(),
            status: task.status().to_string(),
            action: "resumed",
        }),
        None => Err(Error::NotFound),
    }
}

/// Seals the exact attempt of a task as a parked pause checkpoint.
pub(crate) async fn park_paused<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
) -> Result<Task> {
    lock_task_attempt(connection, namespace, queue, attempt).await?;
    let task = task_by_id(connection, namespace, queue, attempt.task_id())
        .await?
        .ok_or(Error::NotFound)?;
    if attempt.pause_generation() > task.pause_generation() {
        return Err(Error::PauseGenerationMismatch {
            task_id: attempt.task_id().as_uuid(),
            expected: task.pause_generation(),
            found: attempt.pause_generation(),
        });
    }
    let row = connection
        .query_one_raw(sql::statement(
            PARK_PAUSED,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
            ],
        ))
        .await?;
    match row {
        Some(row) => task_from_model(task_entity::Model::from_query_result(&row, "")?),
        None => Err(Error::LeaseLost {
            task_id: attempt.task_id().as_uuid(),
        }),
    }
}

/// Seals the attempts whose lease expired and schedules their tasks again.
pub(crate) async fn recover_expired_tasks<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    limit: u32,
) -> Result<Vec<Task>> {
    let rows = connection
        .query_all_raw(sql::statement(
            RECOVER_EXPIRED,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                i64::from(limit).into(),
                Option::<Uuid>::None.into(),
            ],
        ))
        .await?;
    rows.into_iter()
        .map(|row| task_from_model(task_entity::Model::from_query_result(&row, "")?))
        .collect()
}

/// Seals the expired attempt of one task of the queue and schedules it again.
///
/// The recovery never falls back to another task. A target that is absent, out
/// of scope, not expired or sealed by another transaction yields `None`.
pub(crate) async fn recover_expired_task<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    task_id: TaskId,
) -> Result<Option<Task>> {
    let rows = connection
        .query_all_raw(sql::statement(
            RECOVER_EXPIRED,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                1i64.into(),
                task_id.as_uuid().into(),
            ],
        ))
        .await?;
    rows.into_iter()
        .next()
        .map(|row| task_from_model(task_entity::Model::from_query_result(&row, "")?))
        .transpose()
}

pub(crate) fn task_from_model(row: task_entity::Model) -> Result<Task> {
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
        pause_state: TaskPauseState::from_db(&row.pause_state)?,
        pause_generation: row.pause_generation,
        paused_attempts: u32::try_from(row.paused_attempts).unwrap_or_default(),
    })
}

fn attempt_from_model(row: attempt_entity::Model) -> Result<Attempt> {
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
        pause_generation: row.pause_generation,
        paused_at: row.paused_at,
    })
}

fn progress_from_json(value: Value) -> Result<ProgressUpdate> {
    serde_json::from_value(value).map_err(|error| Error::Validation {
        field: "progress",
        reason: format!("the stored progress report is invalid: {error}"),
    })
}

fn uuid_array(task_ids: &[Uuid]) -> SeaValue {
    SeaValue::Array(
        ArrayType::Uuid,
        Some(Box::new(
            task_ids
                .iter()
                .map(|task_id| SeaValue::Uuid(Some(*task_id)))
                .collect(),
        )),
    )
}

fn first_page_cursor() -> (OffsetDateTime, Uuid) {
    let end_of_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(253_402_300_799);
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

async fn fetch_scalar<T>(
    connection: &impl ConnectionTrait,
    statement: Statement,
) -> Result<Option<T>>
where
    T: TryGetable,
{
    match connection.query_one_raw(statement).await? {
        Some(row) => Ok(Some(row.try_get_by::<T, usize>(0)?)),
        None => Ok(None),
    }
}
