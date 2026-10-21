//! The scoped queue of one namespace.
//!
//! A queue addresses the tasks of one namespace and one queue name. Every
//! operation of the queue is scoped that way, so tasks of another namespace
//! or another queue are never visible to it.
//!
//! Opening a queue never creates or alters the schema. The schema must be
//! owned by the library, otherwise the queue refuses to open.

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::{PgConnection, PgPool, Postgres, Transaction};
use std::time::Duration;
use uuid::Uuid;

use crate::backoff;
use crate::config::WorkerConfig;
use crate::error::{Error, Result};
use crate::migrate::check_schema;
use crate::model::{
    Attempt, Claim, EnqueueOutcome, EnqueueRequest, Failure, ListCursor, Namespace, ProgressUpdate,
    QueueName, Task, TaskId,
};
use crate::{idempotency, progress, store};

/// The largest number of tasks a listing returns.
pub const MAX_LIST_LIMIT: u32 = 100;

/// The largest number of attempts a listing returns.
pub const MAX_ATTEMPT_LIMIT: u32 = 100;

/// The largest number of tasks a recovery pass handles.
pub const MAX_RECOVERY_LIMIT: u32 = 1000;

/// The largest length of a worker identifier.
pub const MAX_WORKER_ID_LENGTH: usize = 128;

/// A queue of one namespace and one queue name.
#[derive(Debug, Clone)]
pub struct Queue {
    pool: PgPool,
    namespace: Namespace,
    queue: QueueName,
    lease_ttl: Duration,
    instance: Uuid,
}

impl Queue {
    /// Opens a queue over a pool.
    ///
    /// The pool must point at a database whose schema is owned by this
    /// library. The lease time to live of the claims of this queue is taken
    /// from the validated configuration.
    pub async fn open(
        pool: PgPool,
        namespace: &str,
        queue: &str,
        config: &WorkerConfig,
    ) -> Result<Self> {
        config.validate()?;
        let namespace = Namespace::new(namespace)?;
        let queue = QueueName::new(queue)?;
        let info = check_schema(&pool).await?;
        Ok(Self {
            pool,
            namespace,
            queue,
            lease_ttl: config.lease_ttl,
            instance: info.instance_uuid(),
        })
    }

    /// The pool of the queue.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The namespace of the queue.
    pub fn namespace(&self) -> &Namespace {
        &self.namespace
    }

    /// The queue name of the queue.
    pub fn queue(&self) -> &QueueName {
        &self.queue
    }

    /// The lease time to live of the claims of this queue.
    pub fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    /// Enqueues a task.
    ///
    /// The task and its wake up notification are committed together.
    pub async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome> {
        let mut transaction = self.pool.begin().await?;
        let outcome =
            idempotency::enqueue_task(&mut transaction, &self.namespace, &self.queue, &request)
                .await;
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

    /// Enqueues a task in the transaction of the caller.
    ///
    /// The task and its wake up notification are only durable when the caller
    /// commits the transaction. The transaction must belong to the schema
    /// instance of this queue, otherwise the call is refused.
    pub async fn enqueue_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        request: EnqueueRequest,
    ) -> Result<EnqueueOutcome> {
        self.verify_instance(transaction).await?;
        idempotency::enqueue_task(transaction, &self.namespace, &self.queue, &request).await
    }

    /// Reads one task of the queue.
    pub async fn get(&self, task_id: TaskId) -> Result<Option<Task>> {
        let mut connection = self.pool.acquire().await?;
        store::task_by_id(&mut connection, &self.namespace, &self.queue, task_id).await
    }

    /// Lists the tasks of the queue, newest first.
    ///
    /// The listing is bounded and continues after the given cursor.
    pub async fn list(&self, cursor: Option<ListCursor>, limit: u32) -> Result<Vec<Task>> {
        let limit = validate_limit(limit, MAX_LIST_LIMIT, "limit")?;
        let mut connection = self.pool.acquire().await?;
        store::list_tasks(&mut connection, &self.namespace, &self.queue, cursor, limit).await
    }

    /// Lists the attempts of a task, newest first.
    pub async fn attempts(&self, task_id: TaskId, limit: u32) -> Result<Vec<Attempt>> {
        let limit = validate_limit(limit, MAX_ATTEMPT_LIMIT, "limit")?;
        let mut connection = self.pool.acquire().await?;
        store::list_attempts(
            &mut connection,
            &self.namespace,
            &self.queue,
            task_id,
            limit,
        )
        .await
    }

    /// Claims the next ready task of the queue.
    ///
    /// The claim holds the lease of the task until it expires, and carries
    /// the fencing token of the new attempt.
    pub async fn claim(&self, worker_id: &str) -> Result<Option<Claim>> {
        let worker_id = validate_worker_id(worker_id)?;
        let mut connection = self.pool.acquire().await?;
        self.claim_on(&mut connection, worker_id, None).await
    }

    /// Claims the next ready task of the queue in the transaction of the
    /// caller.
    ///
    /// The claim and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn claim_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        worker_id: &str,
    ) -> Result<Option<Claim>> {
        let worker_id = validate_worker_id(worker_id)?;
        self.verify_instance(transaction).await?;
        self.claim_on(transaction, worker_id, None).await
    }

    /// Claims one task of the queue by its identifier.
    ///
    /// The task must be ready and of this queue, otherwise the call reports
    /// `None` and never claims another task of the queue. The claim holds the
    /// lease of the task until it expires, and carries the fencing token of
    /// the new attempt.
    ///
    /// The call bypasses the order of the queue for this one task. The
    /// decision which task is eligible stays with the caller.
    pub async fn claim_by_id(&self, task_id: TaskId, worker_id: &str) -> Result<Option<Claim>> {
        let worker_id = validate_worker_id(worker_id)?;
        let mut connection = self.pool.acquire().await?;
        self.claim_on(&mut connection, worker_id, Some(task_id))
            .await
    }

    /// Claims one task of the queue by its identifier in the transaction of
    /// the caller.
    ///
    /// The claim and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn claim_by_id_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        task_id: TaskId,
        worker_id: &str,
    ) -> Result<Option<Claim>> {
        let worker_id = validate_worker_id(worker_id)?;
        self.verify_instance(transaction).await?;
        self.claim_on(transaction, worker_id, Some(task_id)).await
    }

    /// Renews the lease of an attempt.
    ///
    /// The lock of the attempt and the renewal run in one transaction, so
    /// that the lease is checked against the clock of the database after the
    /// lock was taken.
    pub async fn renew(&self, attempt: &Attempt) -> Result<DateTime<Utc>> {
        let mut transaction = self.pool.begin().await?;
        let outcome = self.renew_on(&mut transaction, attempt).await;
        match outcome {
            Ok(expiry) => {
                transaction.commit().await?;
                Ok(expiry)
            }
            Err(error) => {
                transaction.rollback().await.ok();
                Err(error)
            }
        }
    }

    /// Renews the lease of an attempt in the transaction of the caller.
    ///
    /// The renewal and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused. The lock
    /// of the attempt is held until the caller ends its transaction, so the
    /// lease is checked against the clock of the database after the lock was
    /// taken.
    pub async fn renew_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        attempt: &Attempt,
    ) -> Result<DateTime<Utc>> {
        self.verify_instance(transaction).await?;
        self.renew_on(transaction, attempt).await
    }

    /// Writes the progress report of a running attempt.
    ///
    /// The lock of the attempt and the report run in one transaction, so that
    /// the lease is checked against the clock of the database after the lock
    /// was taken.
    pub async fn report_progress(&self, attempt: &Attempt, report: &ProgressUpdate) -> Result<()> {
        report.validate()?;
        let mut transaction = self.pool.begin().await?;
        let outcome = self
            .report_progress_on(&mut transaction, attempt, report)
            .await;
        match outcome {
            Ok(()) => {
                transaction.commit().await?;
                Ok(())
            }
            Err(error) => {
                transaction.rollback().await.ok();
                Err(error)
            }
        }
    }

    /// Writes the progress report of a running attempt in the transaction of
    /// the caller.
    ///
    /// The report and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused. The lock
    /// of the attempt is held until the caller ends its transaction, so the
    /// lease is checked against the clock of the database after the lock was
    /// taken.
    pub async fn report_progress_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        attempt: &Attempt,
        report: &ProgressUpdate,
    ) -> Result<()> {
        report.validate()?;
        self.verify_instance(transaction).await?;
        self.report_progress_on(transaction, attempt, report).await
    }

    /// Completes an attempt and the task it belongs to.
    ///
    /// A task whose cancellation was requested before the completion ends as
    /// cancelled and its result is discarded.
    pub async fn complete(&self, attempt: &Attempt, result: Option<Value>) -> Result<Task> {
        let mut transaction = self.pool.begin().await?;
        let outcome = store::complete_attempt(
            &mut transaction,
            &self.namespace,
            &self.queue,
            attempt,
            result,
        )
        .await;
        match outcome {
            Ok(task) => {
                transaction.commit().await?;
                Ok(task)
            }
            Err(error) => {
                transaction.rollback().await.ok();
                Err(error)
            }
        }
    }

    /// Completes an attempt in the transaction of the caller.
    ///
    /// The completion and the business write of the caller are only durable
    /// when the caller commits the transaction. A refused completion must not
    /// be ignored, otherwise the caller commits business side effects of an
    /// attempt that lost its lease.
    pub async fn complete_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        attempt: &Attempt,
        result: Option<Value>,
    ) -> Result<Task> {
        self.verify_instance(transaction).await?;
        store::complete_attempt(transaction, &self.namespace, &self.queue, attempt, result).await
    }

    /// Fails an attempt.
    ///
    /// A retryable failure schedules the task again while the budget of the
    /// task allows it, a permanent failure ends the task.
    pub async fn fail(&self, attempt: &Attempt, failure: &Failure) -> Result<Task> {
        let mut transaction = self.pool.begin().await?;
        let outcome = self
            .fail_in_transaction(&mut transaction, attempt, failure)
            .await;
        match outcome {
            Ok(task) => {
                transaction.commit().await?;
                Ok(task)
            }
            Err(error) => {
                transaction.rollback().await.ok();
                Err(error)
            }
        }
    }

    /// Fails an attempt in the transaction of the caller.
    pub async fn fail_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        attempt: &Attempt,
        failure: &Failure,
    ) -> Result<Task> {
        self.verify_instance(transaction).await?;
        self.fail_in_transaction(transaction, attempt, failure)
            .await
    }

    /// Requests the cancellation of a task.
    ///
    /// A queued task is cancelled at once, a running task is marked for
    /// cancellation, and a terminal task is reported unchanged.
    pub async fn request_cancel(&self, task_id: TaskId) -> Result<Task> {
        let mut connection = self.pool.acquire().await?;
        self.cancel_on(&mut connection, task_id).await
    }

    /// Requests the cancellation of a task in the transaction of the caller.
    ///
    /// The cancellation and the business write of the caller are only durable
    /// when the caller commits the transaction. The transaction must belong to
    /// the schema instance of this queue, otherwise the call is refused.
    pub async fn request_cancel_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        task_id: TaskId,
    ) -> Result<Task> {
        self.verify_instance(transaction).await?;
        self.cancel_on(transaction, task_id).await
    }

    /// Schedules a failed task again with a larger budget.
    ///
    /// The new budget of the task is the number of the attempts that ran plus
    /// the requested additional attempts. An unused budget of the task is not
    /// granted again, so the budget can shrink.
    ///
    /// The attempt history of the task and its request stay untouched. A task
    /// that is not failed is not retried.
    pub async fn retry_failed(&self, task_id: TaskId, additional_attempts: u32) -> Result<Task> {
        let additional_attempts = validate_additional_attempts(additional_attempts)?;
        let mut connection = self.pool.acquire().await?;
        self.retry_failed_on(&mut connection, task_id, additional_attempts)
            .await
    }

    /// Schedules a failed task again with a larger budget in the transaction
    /// of the caller.
    ///
    /// The retry and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn retry_failed_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        task_id: TaskId,
        additional_attempts: u32,
    ) -> Result<Task> {
        let additional_attempts = validate_additional_attempts(additional_attempts)?;
        self.verify_instance(transaction).await?;
        self.retry_failed_on(transaction, task_id, additional_attempts)
            .await
    }

    /// Seals the attempts whose lease expired and schedules their tasks again.
    ///
    /// The pass is bounded and only touches the tasks of this queue. A task
    /// whose cancellation was requested ends as cancelled, a task without
    /// remaining budget ends as failed.
    pub async fn recover_expired(&self, limit: u32) -> Result<Vec<Task>> {
        let limit = validate_limit(limit, MAX_RECOVERY_LIMIT, "limit")?;
        let mut connection = self.pool.acquire().await?;
        self.recover_expired_on(&mut connection, limit).await
    }

    /// Seals the attempts whose lease expired and schedules their tasks again
    /// in the transaction of the caller.
    ///
    /// The recovery and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn recover_expired_in(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        limit: u32,
    ) -> Result<Vec<Task>> {
        let limit = validate_limit(limit, MAX_RECOVERY_LIMIT, "limit")?;
        self.verify_instance(transaction).await?;
        self.recover_expired_on(transaction, limit).await
    }

    async fn claim_on(
        &self,
        connection: &mut PgConnection,
        worker_id: &str,
        target: Option<TaskId>,
    ) -> Result<Option<Claim>> {
        store::claim_task(
            connection,
            &self.namespace,
            &self.queue,
            worker_id,
            self.lease_ttl,
            target,
        )
        .await
    }

    async fn renew_on(
        &self,
        connection: &mut PgConnection,
        attempt: &Attempt,
    ) -> Result<DateTime<Utc>> {
        store::renew_attempt(
            connection,
            &self.namespace,
            &self.queue,
            attempt,
            self.lease_ttl,
        )
        .await
    }

    async fn report_progress_on(
        &self,
        connection: &mut PgConnection,
        attempt: &Attempt,
        report: &ProgressUpdate,
    ) -> Result<()> {
        store::lock_attempt(connection, &self.namespace, &self.queue, attempt).await?;
        progress::report_progress(connection, &self.namespace, &self.queue, attempt, report).await
    }

    async fn cancel_on(&self, connection: &mut PgConnection, task_id: TaskId) -> Result<Task> {
        store::cancel_task(connection, &self.namespace, &self.queue, task_id).await
    }

    async fn retry_failed_on(
        &self,
        connection: &mut PgConnection,
        task_id: TaskId,
        additional_attempts: u32,
    ) -> Result<Task> {
        store::retry_failed_task(
            connection,
            &self.namespace,
            &self.queue,
            task_id,
            additional_attempts,
        )
        .await
    }

    async fn recover_expired_on(
        &self,
        connection: &mut PgConnection,
        limit: u32,
    ) -> Result<Vec<Task>> {
        store::recover_expired_tasks(connection, &self.namespace, &self.queue, limit).await
    }

    async fn fail_in_transaction(
        &self,
        transaction: &mut Transaction<'_, Postgres>,
        attempt: &Attempt,
        failure: &Failure,
    ) -> Result<Task> {
        let task = store::task_by_id(transaction, &self.namespace, &self.queue, attempt.task_id())
            .await?
            .ok_or(Error::NotFound)?;
        let delay = backoff::delay_for(
            task.retry_initial_delay(),
            task.retry_max_delay(),
            attempt.attempt_number(),
        );
        store::fail_attempt(
            transaction,
            &self.namespace,
            &self.queue,
            attempt,
            failure,
            delay,
        )
        .await
    }

    async fn verify_instance(&self, connection: &mut PgConnection) -> Result<()> {
        store::verify_instance(connection, self.instance).await
    }
}

fn validate_limit(limit: u32, max: u32, field: &'static str) -> Result<u32> {
    if limit == 0 || limit > max {
        return Err(Error::Validation {
            field,
            reason: format!("must be between 1 and {max}"),
        });
    }
    Ok(limit)
}

fn validate_worker_id(worker_id: &str) -> Result<&str> {
    if worker_id.is_empty() || worker_id.len() > MAX_WORKER_ID_LENGTH {
        return Err(Error::Validation {
            field: "worker_id",
            reason: format!("must be between 1 and {MAX_WORKER_ID_LENGTH} bytes"),
        });
    }
    Ok(worker_id)
}

fn validate_additional_attempts(additional_attempts: u32) -> Result<u32> {
    if additional_attempts == 0 {
        return Err(Error::Validation {
            field: "additional_attempts",
            reason: "must be at least 1".to_owned(),
        });
    }
    if additional_attempts > store::MAX_ADDITIONAL_ATTEMPTS {
        return Err(Error::Validation {
            field: "additional_attempts",
            reason: format!("must not exceed {}", store::MAX_ADDITIONAL_ATTEMPTS),
        });
    }
    Ok(additional_attempts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_are_bounded() {
        assert!(validate_limit(0, MAX_LIST_LIMIT, "limit").is_err());
        assert!(validate_limit(MAX_LIST_LIMIT + 1, MAX_LIST_LIMIT, "limit").is_err());
        assert!(validate_limit(MAX_LIST_LIMIT, MAX_LIST_LIMIT, "limit").is_ok());
    }

    #[test]
    fn worker_identifiers_are_bounded() {
        assert!(validate_worker_id("").is_err());
        assert!(validate_worker_id(&"w".repeat(MAX_WORKER_ID_LENGTH + 1)).is_err());
        assert!(validate_worker_id("worker-1").is_ok());
    }
}
