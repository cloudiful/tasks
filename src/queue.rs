//! The scoped queue of one namespace.
//!
//! A queue addresses the tasks of one namespace and one queue name. Every
//! operation of the queue is scoped that way, so tasks of another namespace
//! or another queue are never visible to it.
//!
//! Opening a queue never creates or alters the schema. The schema must be
//! owned by the library, otherwise the queue refuses to open.

use sea_orm::{ConnectionTrait, DatabaseConnection, TransactionTrait};
use serde_json::Value;
use std::time::Duration;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::backoff;
use crate::config::WorkerConfig;
use crate::error::{Error, Result};
use crate::migrate::check_schema;
use crate::model::{
    Attempt, Claim, EnqueueOutcome, EnqueueRequest, Failure, ListCursor, Namespace, ProgressUpdate,
    QueueName, Task, TaskId, TaskKind,
};
use crate::transaction::TaskTransaction;
use crate::{idempotency, progress, store};

/// The largest number of tasks a listing returns.
pub const MAX_LIST_LIMIT: u32 = 100;

/// The largest number of attempts a listing returns.
pub const MAX_ATTEMPT_LIMIT: u32 = 100;

/// The largest number of tasks a recovery pass handles.
pub const MAX_RECOVERY_LIMIT: u32 = 1000;

/// The largest number of task identifiers a batch read accepts.
///
/// The limit bounds the input of one call and keeps its statement and its row
/// locks inside the resources of one transaction. A caller that holds more
/// identifiers passes them in several batches of a known, sorted set.
pub const MAX_TASK_BATCH_SIZE: usize = 512;

/// The largest lease grace an activity read accepts.
///
/// The grace widens the window in which an attempt counts as active, so a
/// larger grace reports more activity. A grace of at most one hour keeps the
/// comparison inside a bounded interval.
pub const MAX_ACTIVITY_LEASE_GRACE: Duration = Duration::from_secs(3600);

/// The largest length of a worker identifier.
pub const MAX_WORKER_ID_LENGTH: usize = 128;

/// The durable execution policy of one namespace and queue.
///
/// The policy applies to every instance that opens the same queue. A missing
/// policy row is the same as [`QueuePolicy::unlimited`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct QueuePolicy {
    /// The largest number of attempts that may run at the same time.
    ///
    /// `None` keeps the queue unlimited. `Some(limit)` must be positive and
    /// fit the integer column of the queue policy table.
    pub max_concurrent_attempts: Option<u32>,
}

impl QueuePolicy {
    /// A policy without a global concurrency limit.
    pub const fn unlimited() -> Self {
        Self {
            max_concurrent_attempts: None,
        }
    }

    fn validate(&self) -> Result<()> {
        if let Some(limit) = self.max_concurrent_attempts {
            if limit == 0 {
                return Err(Error::Validation {
                    field: "max_concurrent_attempts",
                    reason: "must be at least 1".to_owned(),
                });
            }
            if limit > i32::MAX as u32 {
                return Err(Error::Validation {
                    field: "max_concurrent_attempts",
                    reason: format!("must not exceed {}", i32::MAX),
                });
            }
        }
        Ok(())
    }
}

/// A queue of one namespace and one queue name.
#[derive(Debug, Clone)]
pub struct Queue {
    db: DatabaseConnection,
    namespace: Namespace,
    queue: QueueName,
    lease_ttl: Duration,
    instance: Uuid,
}

impl Queue {
    /// Opens a queue over a database connection.
    ///
    /// The connection must point at a database whose schema is owned by this
    /// library. The lease time to live of the claims of this queue is taken
    /// from the validated configuration.
    pub async fn open(
        db: DatabaseConnection,
        namespace: &str,
        queue: &str,
        config: &WorkerConfig,
    ) -> Result<Self> {
        config.validate()?;
        let namespace = Namespace::new(namespace)?;
        let queue = QueueName::new(queue)?;
        let info = check_schema(&db).await?;
        Ok(Self {
            db,
            namespace,
            queue,
            lease_ttl: config.lease_ttl,
            instance: info.instance_uuid(),
        })
    }

    /// The database connection of the queue.
    pub fn connection(&self) -> &DatabaseConnection {
        &self.db
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

    /// Reads the durable execution policy of the queue.
    ///
    /// The policy is read from the database on every call. A queue without a
    /// stored policy is unlimited.
    pub async fn policy(&self) -> Result<QueuePolicy> {
        store::queue_policy(&self.db, &self.namespace, &self.queue).await
    }

    /// Replaces the durable execution policy of the queue.
    ///
    /// The replacement and every claim of the queue are serialized by the
    /// queue scoped transaction lock. A lower limit applies to future claims
    /// and never cancels running attempts. `None` removes the policy row and
    /// restores the unlimited behavior.
    pub async fn set_policy(&self, policy: QueuePolicy) -> Result<()> {
        policy.validate()?;
        let transaction = self.db.begin().await?;
        let outcome = self.set_policy_on(&transaction, policy).await;
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

    /// Enqueues a task.
    ///
    /// The task and its wake up notification are committed together.
    pub async fn enqueue(&self, request: EnqueueRequest) -> Result<EnqueueOutcome> {
        let transaction = self.db.begin().await?;
        let outcome =
            idempotency::enqueue_task(&transaction, &self.namespace, &self.queue, &request).await;
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
    ///
    /// The transaction is a SeaORM transaction or a native SQLx transaction of
    /// the caller. The library never commits or rolls it back.
    pub async fn enqueue_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        request: EnqueueRequest,
    ) -> Result<EnqueueOutcome> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        idempotency::enqueue_task(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            &request,
        )
        .await
    }

    /// Reads one task of the queue.
    pub async fn get(&self, task_id: TaskId) -> Result<Option<Task>> {
        store::task_by_id(&self.db, &self.namespace, &self.queue, task_id).await
    }

    /// Reads one task of the queue in the transaction of the caller.
    ///
    /// The read takes no row lock. The transaction must belong to the schema
    /// instance of this queue, otherwise the call is refused.
    pub async fn get_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
    ) -> Result<Option<Task>> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        store::task_by_id(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            task_id,
        )
        .await
    }

    /// Locks one task of the queue in the transaction of the caller.
    ///
    /// The task is locked for update until the caller ends its transaction, so
    /// a claim, a recovery and a terminal writer of the task wait for the
    /// caller. A task that is absent or belongs to another namespace or queue
    /// is reported as `NotFound` and never locked.
    pub async fn lock_task_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        store::lock_task(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            task_id,
        )
        .await
    }

    /// Reads a batch of tasks of the queue in the transaction of the caller.
    ///
    /// The read takes no row lock. The batch is limited to
    /// [`MAX_TASK_BATCH_SIZE`] identifiers before any database work, and
    /// duplicate identifiers are read once. A task that is absent or belongs
    /// to another namespace or queue is omitted, like a [`Queue::get`] call
    /// that reports `None`. The tasks are ordered by their identifier.
    pub async fn get_many_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_ids: &[TaskId],
    ) -> Result<Vec<Task>> {
        let task_ids = validate_task_ids(task_ids)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        store::tasks_by_ids(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            &task_ids,
        )
        .await
    }

    /// Reports whether any task of a batch holds a live attempt in the
    /// transaction of the caller.
    ///
    /// Every distinct task of the batch is locked for share in identifier
    /// order until the caller ends its transaction, so a claim, a recovery and
    /// a terminal writer of those tasks wait for the caller. A task that is
    /// absent or belongs to another namespace or queue is reported as
    /// `NotFound`; the call never reports `false` for a batch it could not
    /// verify. An attempt is live while it runs and its lease expires in the
    /// future of the transaction clock. The batch is limited to
    /// [`MAX_TASK_BATCH_SIZE`] identifiers before any database work.
    pub async fn has_live_attempts_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_ids: &[TaskId],
    ) -> Result<bool> {
        let task_ids = validate_task_ids(task_ids)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        if task_ids.is_empty() {
            return Ok(false);
        }
        store::has_live_attempts(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            &task_ids,
        )
        .await
    }

    /// Reads the active tasks of one kind in the transaction of the caller.
    ///
    /// A task is active when its status is queued, running or cancel
    /// requested, or when it holds a running attempt whose lease expires later
    /// than the grace of the caller before the clock of the transaction. The
    /// tasks belong to the namespace and the queue of this handle and carry
    /// the given kind. Their payload stays opaque.
    ///
    /// The read takes no row lock. A snapshot alone does not guarantee that no
    /// task of the kind appears between the read and a later write. A caller
    /// that needs that guarantee must exclude the creation, the adoption and
    /// the reactivation of a task with its own locks, in a consistent order,
    /// before it uses this read.
    ///
    /// The read is complete or it fails. More than
    /// [`MAX_TASK_BATCH_SIZE`] matching tasks are refused with
    /// [`Error::Validation`] instead of truncated, so a caller never treats a
    /// partial result as an absence of activity. An empty result is a
    /// complete result.
    ///
    /// The grace must be positive and at most
    /// [`MAX_ACTIVITY_LEASE_GRACE`]. The transaction must belong to the schema
    /// instance of this queue, otherwise the call is refused.
    pub async fn active_tasks_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        kind: &TaskKind,
        lease_grace: Duration,
    ) -> Result<Vec<Task>> {
        let lease_grace = validate_lease_grace(lease_grace)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        store::active_tasks_of_kind(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            kind,
            lease_grace,
        )
        .await
    }

    /// Reads one active task of one kind in the transaction of the caller.
    ///
    /// The predicate is the same as the one of [`Queue::active_tasks_in`]. A
    /// task that is absent or belongs to another namespace, another queue or
    /// another kind is reported as [`Error::NotFound`]. A task of the scope
    /// that is not active is reported as `None`.
    ///
    /// The read takes no row lock. The grace must be positive and at most
    /// [`MAX_ACTIVITY_LEASE_GRACE`]. The transaction must belong to the schema
    /// instance of this queue, otherwise the call is refused.
    pub async fn active_task_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        kind: &TaskKind,
        task_id: TaskId,
        lease_grace: Duration,
    ) -> Result<Option<Task>> {
        let lease_grace = validate_lease_grace(lease_grace)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        store::active_task_of_kind(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            kind,
            task_id,
            lease_grace,
        )
        .await
    }

    /// Lists the tasks of the queue, newest first.
    ///
    /// The listing is bounded and continues after the given cursor.
    pub async fn list(&self, cursor: Option<ListCursor>, limit: u32) -> Result<Vec<Task>> {
        let limit = validate_limit(limit, MAX_LIST_LIMIT, "limit")?;
        store::list_tasks(&self.db, &self.namespace, &self.queue, cursor, limit).await
    }

    /// Lists the attempts of a task, newest first.
    pub async fn attempts(&self, task_id: TaskId, limit: u32) -> Result<Vec<Attempt>> {
        let limit = validate_limit(limit, MAX_ATTEMPT_LIMIT, "limit")?;
        store::list_attempts(&self.db, &self.namespace, &self.queue, task_id, limit).await
    }

    /// Lists the attempts of a task in the transaction of the caller.
    ///
    /// The bounds and the newest first order are the same as
    /// [`Queue::attempts`], and the read takes no row lock. The transaction
    /// must belong to the schema instance of this queue, otherwise the call is
    /// refused.
    pub async fn attempts_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
        limit: u32,
    ) -> Result<Vec<Attempt>> {
        let limit = validate_limit(limit, MAX_ATTEMPT_LIMIT, "limit")?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        store::list_attempts(
            transaction.connection(),
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
        let transaction = self.db.begin().await?;
        let outcome = self.claim_on(&transaction, worker_id, None).await;
        match outcome {
            Ok(claim) => {
                transaction.commit().await?;
                Ok(claim)
            }
            Err(error) => {
                transaction.rollback().await.ok();
                Err(error)
            }
        }
    }

    /// Claims the next ready task of the queue in the transaction of the
    /// caller.
    ///
    /// The claim and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused. The
    /// transaction must run at `READ COMMITTED`; every claim requires it, so
    /// another isolation level is refused before the policy is read.
    pub async fn claim_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        worker_id: &str,
    ) -> Result<Option<Claim>> {
        let worker_id = validate_worker_id(worker_id)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.claim_on(transaction.connection(), worker_id, None)
            .await
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
        let transaction = self.db.begin().await?;
        let outcome = self.claim_on(&transaction, worker_id, Some(task_id)).await;
        match outcome {
            Ok(claim) => {
                transaction.commit().await?;
                Ok(claim)
            }
            Err(error) => {
                transaction.rollback().await.ok();
                Err(error)
            }
        }
    }

    /// Claims one task of the queue by its identifier in the transaction of
    /// the caller.
    ///
    /// The claim and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused. The
    /// transaction must run at `READ COMMITTED`; every claim requires it, so
    /// another isolation level is refused before the policy is read.
    pub async fn claim_by_id_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
        worker_id: &str,
    ) -> Result<Option<Claim>> {
        let worker_id = validate_worker_id(worker_id)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.claim_on(transaction.connection(), worker_id, Some(task_id))
            .await
    }

    /// Renews the lease of an attempt.
    ///
    /// The lock of the attempt and the renewal run in one transaction, so
    /// that the lease is checked against the clock of the database after the
    /// lock was taken.
    pub async fn renew(&self, attempt: &Attempt) -> Result<OffsetDateTime> {
        let transaction = self.db.begin().await?;
        let outcome = self.renew_on(&transaction, attempt).await;
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
        transaction: impl Into<TaskTransaction<'_>>,
        attempt: &Attempt,
    ) -> Result<OffsetDateTime> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.renew_on(transaction.connection(), attempt).await
    }

    /// Writes the progress report of a running attempt.
    ///
    /// The lock of the attempt and the report run in one transaction, so that
    /// the lease is checked against the clock of the database after the lock
    /// was taken.
    pub async fn report_progress(&self, attempt: &Attempt, report: &ProgressUpdate) -> Result<()> {
        report.validate()?;
        let transaction = self.db.begin().await?;
        let outcome = self.report_progress_on(&transaction, attempt, report).await;
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
        transaction: impl Into<TaskTransaction<'_>>,
        attempt: &Attempt,
        report: &ProgressUpdate,
    ) -> Result<()> {
        report.validate()?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.report_progress_on(transaction.connection(), attempt, report)
            .await
    }

    /// Completes an attempt and the task it belongs to.
    ///
    /// A task whose cancellation was requested before the completion ends as
    /// cancelled and its result is discarded.
    pub async fn complete(&self, attempt: &Attempt, result: Option<Value>) -> Result<Task> {
        let transaction = self.db.begin().await?;
        let outcome =
            store::complete_attempt(&transaction, &self.namespace, &self.queue, attempt, result)
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
        transaction: impl Into<TaskTransaction<'_>>,
        attempt: &Attempt,
        result: Option<Value>,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        store::complete_attempt(
            transaction.connection(),
            &self.namespace,
            &self.queue,
            attempt,
            result,
        )
        .await
    }

    /// Fails an attempt.
    ///
    /// A retryable failure schedules the task again while the budget of the
    /// task allows it, a permanent failure ends the task.
    pub async fn fail(&self, attempt: &Attempt, failure: &Failure) -> Result<Task> {
        let transaction = self.db.begin().await?;
        let outcome = self
            .fail_in_transaction(&transaction, attempt, failure)
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
        transaction: impl Into<TaskTransaction<'_>>,
        attempt: &Attempt,
        failure: &Failure,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.fail_in_transaction(transaction.connection(), attempt, failure)
            .await
    }

    /// Requests the cancellation of a task.
    ///
    /// A queued task is cancelled at once, a running task is marked for
    /// cancellation, and a terminal task is reported unchanged.
    pub async fn request_cancel(&self, task_id: TaskId) -> Result<Task> {
        self.cancel_on(&self.db, task_id).await
    }

    /// Requests the cancellation of a task in the transaction of the caller.
    ///
    /// The cancellation and the business write of the caller are only durable
    /// when the caller commits the transaction. The transaction must belong to
    /// the schema instance of this queue, otherwise the call is refused.
    pub async fn request_cancel_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.cancel_on(transaction.connection(), task_id).await
    }

    /// Requests the cooperative pause of one task.
    ///
    /// A queued task is paused at once without an attempt; a running task
    /// enters the pausing state and keeps its lease until its handler parks at
    /// a checkpoint. A task that is already pausing or paused keeps its
    /// generation. A terminal task is refused.
    pub async fn request_pause(&self, task_id: TaskId) -> Result<Task> {
        self.pause_on(&self.db, task_id).await
    }

    /// Requests the cooperative pause of a task in the transaction of the
    /// caller.
    ///
    /// The pause and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn request_pause_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.pause_on(transaction.connection(), task_id).await
    }

    /// Resumes a pausing or paused task.
    ///
    /// A running task keeps its lease and stops reporting a pause; a parked
    /// task is queued again with its available time and attempt history
    /// unchanged. A resume of an unpaused task is an idempotent no-op.
    pub async fn resume(&self, task_id: TaskId) -> Result<Task> {
        self.resume_on(&self.db, task_id).await
    }

    /// Resumes a task in the transaction of the caller.
    ///
    /// The resume and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn resume_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.resume_on(transaction.connection(), task_id).await
    }

    /// Seals the exact attempt of a task as a parked pause checkpoint.
    ///
    /// The attempt must still hold its unexpired lease and fencing token. A
    /// parked attempt is queued again and does not consume the failure budget.
    /// A park that races a resume leaves the task claimable, a cancellation
    /// that was requested before the park wins.
    pub async fn park_paused(&self, attempt: &Attempt) -> Result<Task> {
        let transaction = self.db.begin().await?;
        let outcome = self.park_on(&transaction, attempt).await;
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

    /// Seals a parked pause checkpoint in the transaction of the caller.
    ///
    /// The park and the business write of the caller are only durable when the
    /// caller commits the transaction. A refused park must not be ignored,
    /// otherwise the caller commits business side effects of an attempt that
    /// lost its lease.
    pub async fn park_paused_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        attempt: &Attempt,
    ) -> Result<Task> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.park_on(transaction.connection(), attempt).await
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
        self.retry_failed_on(&self.db, task_id, additional_attempts)
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
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
        additional_attempts: u32,
    ) -> Result<Task> {
        let additional_attempts = validate_additional_attempts(additional_attempts)?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.retry_failed_on(transaction.connection(), task_id, additional_attempts)
            .await
    }

    /// Seals the attempts whose lease expired and schedules their tasks again.
    ///
    /// The pass is bounded and only touches the tasks of this queue. A task
    /// whose cancellation was requested ends as cancelled, a task without
    /// remaining budget ends as failed.
    pub async fn recover_expired(&self, limit: u32) -> Result<Vec<Task>> {
        let limit = validate_limit(limit, MAX_RECOVERY_LIMIT, "limit")?;
        self.recover_expired_on(&self.db, limit).await
    }

    /// Seals the attempts whose lease expired and schedules their tasks again
    /// in the transaction of the caller.
    ///
    /// The recovery and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused.
    pub async fn recover_expired_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        limit: u32,
    ) -> Result<Vec<Task>> {
        let limit = validate_limit(limit, MAX_RECOVERY_LIMIT, "limit")?;
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.recover_expired_on(transaction.connection(), limit)
            .await
    }

    /// Seals the expired attempt of one task of this queue and schedules the
    /// task again.
    ///
    /// Only the current running attempt of the target is recovered, and only
    /// while its lease expired; the call never falls back to another task and
    /// changes nothing for any other task. A target that is absent, of another
    /// namespace or queue, not expired or sealed by another transaction is
    /// reported as `None`. A live lease is never revoked.
    pub async fn recover_expired_by_id(&self, task_id: TaskId) -> Result<Option<Task>> {
        let transaction = self.db.begin().await?;
        let outcome = self.recover_expired_by_id_on(&transaction, task_id).await;
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

    /// Seals the expired attempt of one task of this queue in the transaction
    /// of the caller.
    ///
    /// The recovery and the business write of the caller are only durable when
    /// the caller commits the transaction. The transaction must belong to the
    /// schema instance of this queue, otherwise the call is refused. Like
    /// [`Queue::recover_expired_by_id`], the call never falls back to another
    /// task.
    pub async fn recover_expired_by_id_in(
        &self,
        transaction: impl Into<TaskTransaction<'_>>,
        task_id: TaskId,
    ) -> Result<Option<Task>> {
        let transaction = transaction.into();
        self.verify_instance(transaction.connection()).await?;
        self.recover_expired_by_id_on(transaction.connection(), task_id)
            .await
    }

    async fn set_policy_on<C: ConnectionTrait>(
        &self,
        connection: &C,
        policy: QueuePolicy,
    ) -> Result<()> {
        store::lock_queue(connection, &self.namespace, &self.queue).await?;
        match policy.max_concurrent_attempts {
            Some(limit) => {
                let limit = i32::try_from(limit).map_err(|_| Error::Validation {
                    field: "max_concurrent_attempts",
                    reason: format!("must not exceed {}", i32::MAX),
                })?;
                store::set_queue_policy(connection, &self.namespace, &self.queue, limit).await
            }
            None => store::clear_queue_policy(connection, &self.namespace, &self.queue).await,
        }
    }

    /// Whether the captured attempt already holds a terminal seal.
    ///
    /// The check is scoped to the namespace, the queue, the task, the attempt
    /// and its fencing token, so a terminal of another attempt never matches.
    pub(crate) async fn attempt_is_sealed(&self, attempt: &Attempt) -> Result<bool> {
        store::attempt_is_sealed(&self.db, &self.namespace, &self.queue, attempt).await
    }

    async fn claim_on<C: ConnectionTrait>(
        &self,
        connection: &C,
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

    async fn renew_on<C: ConnectionTrait>(
        &self,
        connection: &C,
        attempt: &Attempt,
    ) -> Result<OffsetDateTime> {
        store::renew_attempt(
            connection,
            &self.namespace,
            &self.queue,
            attempt,
            self.lease_ttl,
        )
        .await
    }

    async fn report_progress_on<C: ConnectionTrait>(
        &self,
        connection: &C,
        attempt: &Attempt,
        report: &ProgressUpdate,
    ) -> Result<()> {
        store::lock_attempt(connection, &self.namespace, &self.queue, attempt).await?;
        progress::report_progress(connection, &self.namespace, &self.queue, attempt, report).await
    }

    async fn cancel_on<C: ConnectionTrait>(&self, connection: &C, task_id: TaskId) -> Result<Task> {
        store::cancel_task(connection, &self.namespace, &self.queue, task_id).await
    }

    async fn pause_on<C: ConnectionTrait>(&self, connection: &C, task_id: TaskId) -> Result<Task> {
        store::lock_queue(connection, &self.namespace, &self.queue).await?;
        store::pause_task(connection, &self.namespace, &self.queue, task_id).await
    }

    async fn resume_on<C: ConnectionTrait>(&self, connection: &C, task_id: TaskId) -> Result<Task> {
        store::lock_queue(connection, &self.namespace, &self.queue).await?;
        store::resume_task(connection, &self.namespace, &self.queue, task_id).await
    }

    async fn park_on<C: ConnectionTrait>(&self, connection: &C, attempt: &Attempt) -> Result<Task> {
        store::park_paused(connection, &self.namespace, &self.queue, attempt).await
    }

    async fn retry_failed_on<C: ConnectionTrait>(
        &self,
        connection: &C,
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

    async fn recover_expired_on<C: ConnectionTrait>(
        &self,
        connection: &C,
        limit: u32,
    ) -> Result<Vec<Task>> {
        store::recover_expired_tasks(connection, &self.namespace, &self.queue, limit).await
    }

    async fn recover_expired_by_id_on<C: ConnectionTrait>(
        &self,
        connection: &C,
        task_id: TaskId,
    ) -> Result<Option<Task>> {
        store::recover_expired_task(connection, &self.namespace, &self.queue, task_id).await
    }

    async fn fail_in_transaction<C: ConnectionTrait>(
        &self,
        connection: &C,
        attempt: &Attempt,
        failure: &Failure,
    ) -> Result<Task> {
        let task = store::task_by_id(connection, &self.namespace, &self.queue, attempt.task_id())
            .await?
            .ok_or(Error::NotFound)?;
        let delay = backoff::delay_for(
            task.retry_initial_delay(),
            task.retry_max_delay(),
            attempt.attempt_number(),
        );
        store::fail_attempt(
            connection,
            &self.namespace,
            &self.queue,
            attempt,
            failure,
            delay,
        )
        .await
    }

    async fn verify_instance<C: ConnectionTrait>(&self, connection: &C) -> Result<()> {
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

fn validate_task_ids(task_ids: &[TaskId]) -> Result<Vec<Uuid>> {
    if task_ids.len() > MAX_TASK_BATCH_SIZE {
        return Err(Error::Validation {
            field: "task_ids",
            reason: format!("must not exceed {MAX_TASK_BATCH_SIZE} identifiers"),
        });
    }
    let mut task_ids: Vec<Uuid> = task_ids.iter().map(|task_id| task_id.as_uuid()).collect();
    task_ids.sort_unstable();
    task_ids.dedup();
    Ok(task_ids)
}

fn validate_lease_grace(lease_grace: Duration) -> Result<Duration> {
    if lease_grace.is_zero() || lease_grace > MAX_ACTIVITY_LEASE_GRACE {
        return Err(Error::Validation {
            field: "lease_grace",
            reason: format!("must be between 1 microsecond and {MAX_ACTIVITY_LEASE_GRACE:?}"),
        });
    }
    Ok(lease_grace)
}

fn validate_worker_id(worker_id: &str) -> Result<&str> {
    if worker_id.is_empty() || worker_id.len() > MAX_WORKER_ID_LENGTH {
        return Err(Error::Validation {
            field: "worker_id",
            reason: format!("must be between 1 and {} bytes", MAX_WORKER_ID_LENGTH),
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

    #[test]
    fn task_id_batches_are_bounded_deduplicated_and_sorted() {
        let first = TaskId::from_uuid(Uuid::from_u128(1));
        let second = TaskId::from_uuid(Uuid::from_u128(2));

        assert_eq!(
            validate_task_ids(&[second, first, second]).unwrap(),
            vec![first.as_uuid(), second.as_uuid()]
        );
        assert!(validate_task_ids(&[first; MAX_TASK_BATCH_SIZE]).is_ok());
        assert!(validate_task_ids(&[first; MAX_TASK_BATCH_SIZE + 1]).is_err());
    }

    #[test]
    fn activity_lease_grace_is_bounded() {
        assert!(validate_lease_grace(Duration::ZERO).is_err());
        assert!(validate_lease_grace(Duration::from_micros(1)).is_ok());
        assert!(validate_lease_grace(MAX_ACTIVITY_LEASE_GRACE).is_ok());
        assert!(validate_lease_grace(MAX_ACTIVITY_LEASE_GRACE + Duration::from_micros(1)).is_err());
    }
}
