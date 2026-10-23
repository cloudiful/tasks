//! Live integration tests for the worker runtime.
//!
//! The tests need the fixture database of the tasks checkout. They are
//! ignored by default and run with
//! `CLOUDIFUL_TASKS_TEST_DATABASE_URL=<url> cargo test --test worker -- --include-ignored --test-threads=1`.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use cloudiful_tasks::{
    AttemptStatus, CancellationToken, EnqueueRequest, Error, Failure, HandlerError, Outcome,
    ProgressUpdate, Queue, TaskContext, TaskHandler, TaskKind, TaskStatus, Worker, WorkerConfig,
};
use serde_json::json;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use uuid::Uuid;

use common::{attempt_count, connect, open_queue, reset, task_count};

const NAMESPACE: &str = "ns";
const QUEUE: &str = "q";
const KIND: &str = "demo";

/// The events of the handlers of a test.
#[derive(Clone, Default)]
struct Recorder {
    handled: Arc<Mutex<Vec<Uuid>>>,
    cancelled: Arc<Mutex<Vec<Uuid>>>,
    dropped: Arc<Mutex<Vec<Uuid>>>,
    notify: Arc<Notify>,
}

impl Recorder {
    fn handled(&self, task_id: Uuid) {
        self.handled
            .lock()
            .expect("the recorder is not poisoned")
            .push(task_id);
        self.notify.notify_waiters();
    }

    fn cancelled(&self, task_id: Uuid) {
        self.cancelled
            .lock()
            .expect("the recorder is not poisoned")
            .push(task_id);
        self.notify.notify_waiters();
    }

    /// Reports that a handler was dropped without finishing its work.
    fn dropped(&self, task_id: Uuid) {
        self.dropped
            .lock()
            .expect("the recorder is not poisoned")
            .push(task_id);
        self.notify.notify_waiters();
    }

    fn dropped_count(&self) -> usize {
        self.dropped
            .lock()
            .expect("the recorder is not poisoned")
            .len()
    }

    /// Whether the handler of a task was dropped.
    fn was_dropped(&self, task_id: Uuid) -> bool {
        self.dropped
            .lock()
            .expect("the recorder is not poisoned")
            .contains(&task_id)
    }

    fn handled_count(&self) -> usize {
        self.handled
            .lock()
            .expect("the recorder is not poisoned")
            .len()
    }

    fn cancelled_count(&self) -> usize {
        self.cancelled
            .lock()
            .expect("the recorder is not poisoned")
            .len()
    }

    /// Waits until the handlers handled `count` tasks.
    async fn wait_for_handled(&self, count: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.handled_count() < count {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "the handlers did not run in time");
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }

    /// Waits until the handler of a task was dropped.
    async fn wait_for_dropped_of(&self, task_id: Uuid) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while !self.was_dropped(task_id) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "the handler was not dropped in time");
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }

    /// Waits until a handler was dropped.
    async fn wait_for_dropped(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.dropped_count() == 0 {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(!remaining.is_zero(), "the handler was not dropped in time");
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }

    /// Waits until a handler observed the cancellation.
    async fn wait_for_cancelled(&self) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while self.cancelled_count() == 0 {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "the handler was not cancelled in time"
            );
            tokio::select! {
                _ = self.notify.notified() => {}
                _ = tokio::time::sleep(remaining) => {}
            }
        }
    }
}

/// A handler that records the task and succeeds.
struct Recording {
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for Recording {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        Ok(Outcome::completed(Some(json!({"handled": true}))))
    }
}

/// A handler that panics.
struct Panicking {
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for Panicking {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        panic!("the handler panicked");
    }
}

/// A handler that fails with a fixed failure.
struct Failing {
    recorder: Recorder,
    failure: Failure,
}

#[async_trait]
impl TaskHandler for Failing {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        Ok(Outcome::failed(self.failure.clone()))
    }
}

/// A handler that waits for the cancellation of its attempt.
struct Waiting {
    recorder: Recorder,
    work: Duration,
}

#[async_trait]
impl TaskHandler for Waiting {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        let cancel = context.cancellation_token();
        tokio::select! {
            _ = cancel.cancelled() => {
                self.recorder.cancelled(context.task_id().as_uuid());
                Ok(Outcome::completed(None))
            }
            _ = tokio::time::sleep(self.work) => Ok(Outcome::completed(None)),
        }
    }
}

/// A handler that fails retryably on its first attempt and succeeds on the
/// next one.
struct Retrying {
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for Retrying {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        if context.attempt_number() == 1 {
            return Ok(Outcome::failed(Failure::retryable(Some(json!({
                "reason": "boom"
            })))));
        }
        Ok(Outcome::completed(Some(json!({"handled": true}))))
    }
}

/// A guard that reports the drop of a handler.
struct DropSignal {
    recorder: Recorder,
    task_id: Uuid,
}

impl Drop for DropSignal {
    fn drop(&mut self) {
        self.recorder.dropped(self.task_id);
    }
}

/// A handler that ignores the cancellation of its attempt.
struct Ignoring {
    recorder: Recorder,
    work: Duration,
}

#[async_trait]
impl TaskHandler for Ignoring {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        let _signal = DropSignal {
            recorder: self.recorder.clone(),
            task_id: context.task_id().as_uuid(),
        };
        self.recorder.handled(context.task_id().as_uuid());
        tokio::time::sleep(self.work).await;
        Ok(Outcome::completed(None))
    }
}

/// A handler that reports progress and then waits.
struct Reporting {
    recorder: Recorder,
    work: Duration,
}

#[async_trait]
impl TaskHandler for Reporting {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        context
            .progress(
                &ProgressUpdate::new("import")
                    .with_completed(1)
                    .with_total(4),
            )
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        tokio::time::sleep(self.work).await;
        Ok(Outcome::completed(None))
    }
}

/// A handler that commits its own terminal state and reports it.
struct ExternalCompletion {
    queue: Queue,
    recorder: Recorder,
    result: serde_json::Value,
}

#[async_trait]
impl TaskHandler for ExternalCompletion {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        self.queue
            .complete(context.attempt(), Some(self.result.clone()))
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(Outcome::ExternallyCompleted)
    }
}

/// A handler that commits its own failure and reports it.
struct ExternalFailure {
    queue: Queue,
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for ExternalFailure {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        self.queue
            .fail(
                context.attempt(),
                &Failure::permanent(Some(json!({"reason": "external failure"}))),
            )
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(Outcome::ExternallyCompleted)
    }
}

/// A handler that loses a cancellation race before it reports its terminal.
struct ExternalCancellation {
    queue: Queue,
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for ExternalCancellation {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        self.queue
            .request_cancel(context.task_id())
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        self.queue
            .complete(context.attempt(), Some(json!({"discarded": true})))
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(Outcome::ExternallyCompleted)
    }
}

/// A handler that reports an external terminal while a newer attempt is sealed.
struct NewerTerminal {
    pool: sqlx::PgPool,
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for NewerTerminal {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        sqlx::query(
            "INSERT INTO cloudiful_tasks.attempts (
                 id, task_id, attempt_number, worker_id, fencing_token,
                 status, lease_expires_at, started_at, finished_at
             )
             VALUES (
                 gen_random_uuid(), $1, 2, 'newer-worker', gen_random_uuid(),
                 'succeeded', NULL, now(), now()
             )",
        )
        .bind(context.task_id().as_uuid())
        .execute(&self.pool)
        .await
        .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(Outcome::ExternallyCompleted)
    }
}

/// A handler that reports an external terminal without committing one.
struct UnsealedExternalCompletion {
    recorder: Recorder,
}

#[async_trait]
impl TaskHandler for UnsealedExternalCompletion {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        self.recorder.handled(context.task_id().as_uuid());
        Ok(Outcome::ExternallyCompleted)
    }
}

struct SealedContinuation {
    queue: Queue,
    recorder: Recorder,
    release: Arc<Notify>,
    token: Arc<Mutex<Option<CancellationToken>>>,
    mode: u8,
}

#[async_trait]
impl TaskHandler for SealedContinuation {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        let _drop = DropSignal {
            recorder: self.recorder.clone(),
            task_id: context.task_id().as_uuid(),
        };
        *self.token.lock().unwrap() = Some(context.cancellation_token());
        let result = match self.mode {
            0 => self
                .queue
                .complete(context.attempt(), Some(json!({"sealed": true})))
                .await
                .map(|_| ()),
            1 => self
                .queue
                .fail(context.attempt(), &Failure::permanent(None))
                .await
                .map(|_| ()),
            _ => {
                let mut tx = self
                    .queue
                    .pool()
                    .begin()
                    .await
                    .map_err(|e| HandlerError::new(e.to_string()))?;
                self.queue
                    .request_cancel_in(&mut tx, context.task_id())
                    .await
                    .map_err(|e| HandlerError::new(e.to_string()))?;
                self.queue
                    .complete_in(&mut tx, context.attempt(), None)
                    .await
                    .map_err(|e| HandlerError::new(e.to_string()))?;
                tx.commit().await.map_err(Error::from)
            }
        };
        result.map_err(|e| HandlerError::new(e.to_string()))?;
        self.recorder.handled(context.task_id().as_uuid());
        self.release.notified().await;
        Ok(Outcome::ExternallyCompleted)
    }
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_sealed_handler_continues_across_renewals_but_shutdown_still_aborts_it() {
    let pool = connect().await;
    for mode in 0..3 {
        for force_shutdown in [false, true] {
            reset(&pool).await;
            let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
            let task = queue
                .enqueue(EnqueueRequest::new(TaskKind::new(KIND).unwrap(), json!({})))
                .await
                .unwrap();
            let recorder = Recorder::default();
            let release = Arc::new(Notify::new());
            let token = Arc::new(Mutex::new(None));
            let worker = Worker::builder(
                queue.clone(),
                WorkerConfig {
                    heartbeat_interval: Duration::from_millis(40),
                    shutdown_grace: Duration::from_millis(100),
                    ..worker_config()
                },
            )
            .handler(
                TaskKind::new(KIND).unwrap(),
                SealedContinuation {
                    queue: queue.clone(),
                    recorder: recorder.clone(),
                    release: release.clone(),
                    token: token.clone(),
                    mode,
                },
            )
            .unwrap()
            .build()
            .unwrap();
            let shutdown = CancellationToken::new();
            let worker_token = shutdown.clone();
            let worker = tokio::spawn(async move { worker.run(worker_token).await });
            recorder.wait_for_handled(1).await;
            let cancel = token.lock().unwrap().clone().unwrap();
            let mut ticks = tokio::time::interval(Duration::from_millis(80));
            for _ in 0..6 {
                ticks.tick().await;
                assert!(
                    !cancel.is_cancelled(),
                    "a verified seal must not cancel its handler"
                );
                assert!(
                    !recorder.was_dropped(task.task().id().as_uuid()),
                    "a sealed handler must continue through renewal ticks"
                );
            }
            if !force_shutdown {
                release.notify_one();
                recorder.wait_for_dropped().await;
            }
            shutdown.cancel();
            tokio::time::timeout(Duration::from_secs(2), worker)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert!(recorder.was_dropped(task.task().id().as_uuid()));
            if force_shutdown {
                assert!(
                    cancel.is_cancelled(),
                    "shutdown still includes sealed handlers"
                );
            }
            let stored = queue.get(task.task().id()).await.unwrap().unwrap();
            assert_eq!(
                stored.status(),
                match mode {
                    0 => TaskStatus::Succeeded,
                    1 => TaskStatus::Failed,
                    _ => TaskStatus::Cancelled,
                }
            );
            assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
        }
    }
}

fn worker_config() -> WorkerConfig {
    WorkerConfig {
        concurrency: 1,
        lease_ttl: Duration::from_secs(10),
        heartbeat_interval: Duration::from_millis(200),
        heartbeat_failure_budget: 3,
        cancel_poll_interval: Duration::from_millis(100),
        recovery_interval: Duration::from_millis(200),
        idle_poll_interval: Duration::from_millis(100),
        shutdown_grace: Duration::from_millis(500),
    }
}

fn fast_config() -> WorkerConfig {
    WorkerConfig {
        idle_poll_interval: Duration::from_secs(30),
        ..worker_config()
    }
}

fn request() -> EnqueueRequest {
    EnqueueRequest::new(TaskKind::new(KIND).unwrap(), json!({"value": 1}))
}

fn kind() -> TaskKind {
    TaskKind::new(KIND).unwrap()
}

/// Starts a worker and returns its shutdown token and its task.
fn start(worker: Worker) -> (CancellationToken, JoinHandle<Result<(), Error>>) {
    let shutdown = CancellationToken::new();
    let token = shutdown.clone();
    let handle = tokio::spawn(async move { worker.run(token).await });
    (shutdown, handle)
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_worker_handles_a_claimed_task() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Succeeded);
    assert_eq!(
        stored
            .result()
            .and_then(|result| result.get("handled").cloned()),
        Some(json!(true))
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn two_workers_claim_a_task_only_once() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let first = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let second = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (first_shutdown, first_handle) = start(first);
    let (second_shutdown, second_handle) = start(second);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(recorder.handled_count(), 1, "the task must be handled once");
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);

    first_shutdown.cancel();
    second_shutdown.cancel();
    first_handle.await.unwrap().unwrap();
    second_handle.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_panicking_handler_fails_the_task_and_the_worker_keeps_running() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let first = queue.enqueue(request()).await.unwrap();
    let second = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Panicking {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let failed = queue.get(first.task().id()).await.unwrap().unwrap();
    assert_eq!(failed.status(), TaskStatus::Failed);
    let failed = queue.get(second.task().id()).await.unwrap().unwrap();
    assert_eq!(failed.status(), TaskStatus::Failed);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_permanent_failure_is_not_retried() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Failing {
                recorder: recorder.clone(),
                failure: Failure::permanent(Some(json!({"reason": "boom"}))),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(recorder.handled_count(), 1, "the task must not run again");
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Failed);
    assert_eq!(attempt_count(&pool, task.task().id().as_uuid()).await, 1);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_retryable_failure_is_retried_while_the_budget_allows_it() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let policy =
        cloudiful_tasks::RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50))
            .unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Failing {
                recorder: recorder.clone(),
                failure: Failure::retryable(Some(json!({"reason": "boom"}))),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(recorder.handled_count(), 2, "the task must run twice");
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Failed);
    assert_eq!(stored.attempt_count(), 2);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_heartbeat_keeps_the_lease_during_long_work() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let config = WorkerConfig {
        lease_ttl: Duration::from_millis(600),
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_failure_budget: 3,
        ..worker_config()
    };
    let worker = Worker::builder(queue.clone(), config)
        .handler(
            kind(),
            Reporting {
                recorder: recorder.clone(),
                work: Duration::from_millis(900),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(
        stored.status(),
        TaskStatus::Succeeded,
        "the lease must survive the work"
    );
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0].progress().is_some(),
        "the progress must be stored"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn repeated_renewal_failures_revoke_the_authority_of_the_attempt() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    // The worker uses its own pool, so that the test can break it.
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(common::test_database_options())
        .await
        .unwrap();
    let config = WorkerConfig {
        lease_ttl: Duration::from_secs(10),
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_failure_budget: 1,
        ..worker_config()
    };
    let worker_queue = Queue::open(worker_pool.clone(), NAMESPACE, QUEUE, &config)
        .await
        .unwrap();
    let worker = Worker::builder(worker_queue, config)
        .handler(
            kind(),
            Ignoring {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    // The database of the worker breaks, its renewals fail.
    worker_pool.close().await;
    // The worker gives up its authority and drops the handler.
    recorder.wait_for_dropped().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_ne!(
        stored.status(),
        TaskStatus::Succeeded,
        "a lost lease must not complete the task"
    );
    assert_eq!(
        stored.status(),
        TaskStatus::Running,
        "the task must wait for the recovery"
    );
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert!(
        attempts[0]
            .lease_expires_at()
            .is_some_and(|expiry| expiry > chrono::Utc::now()),
        "the lease of the database must still be live"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_shutdown_drops_a_handler_that_ignores_the_cancellation() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Ignoring {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    // The handler ignores the cancellation of its attempt, the shutdown must
    // drop it anyway.
    recorder.wait_for_dropped().await;

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(
        stored.status(),
        TaskStatus::Running,
        "the shutdown must not finish the task"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_locked_attempt_bounds_the_renewal_and_drops_the_handler() {
    let pool = connect().await;
    reset(&pool).await;
    let config = WorkerConfig {
        lease_ttl: Duration::from_millis(400),
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_failure_budget: 3,
        ..worker_config()
    };
    let queue = common::open_queue_with_config(&pool, NAMESPACE, QUEUE, &config).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), config)
        .handler(
            kind(),
            Ignoring {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;

    // Another transaction holds the lock of the attempt, the renewal of the
    // worker waits for it.
    let attempt_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM cloudiful_tasks.attempts WHERE task_id = $1 ORDER BY attempt_number DESC",
    )
    .bind(task.task().id().as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut blocker = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM cloudiful_tasks.attempts WHERE id = $1 FOR UPDATE")
        .bind(attempt_id)
        .fetch_one(&mut *blocker)
        .await
        .unwrap();

    // The renewal must not wait past the lease of the attempt, the worker
    // gives up its authority and drops the handler.
    recorder.wait_for_dropped().await;

    blocker.rollback().await.unwrap();
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_ne!(
        stored.status(),
        TaskStatus::Succeeded,
        "a lost lease must not complete the task"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_busy_pool_bounds_the_renewal_and_drops_the_handler() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    // The worker uses its own pool, so that the test can starve it.
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(common::test_database_options())
        .await
        .unwrap();
    let config = WorkerConfig {
        lease_ttl: Duration::from_millis(400),
        heartbeat_interval: Duration::from_millis(50),
        heartbeat_failure_budget: 3,
        ..worker_config()
    };
    let worker_queue = Queue::open(worker_pool.clone(), NAMESPACE, QUEUE, &config)
        .await
        .unwrap();
    let worker = Worker::builder(worker_queue, config)
        .handler(
            kind(),
            Ignoring {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;

    // The connections of the worker are busy, its renewal waits for one.
    let mut busy = Vec::new();
    while let Ok(Ok(connection)) =
        tokio::time::timeout(Duration::from_millis(200), worker_pool.acquire()).await
    {
        busy.push(connection);
    }

    // The renewal must not wait past the lease of the attempt, the worker
    // gives up its authority and drops the handler.
    recorder.wait_for_dropped().await;

    drop(busy);
    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_cancellation_stops_the_handler() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Waiting {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    queue.request_cancel(task.task().id()).await.unwrap();
    recorder.wait_for_cancelled().await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Cancelled);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_shutdown_leaves_the_lease_to_the_recovery() {
    let pool = connect().await;
    reset(&pool).await;
    let config = WorkerConfig {
        lease_ttl: Duration::from_millis(300),
        heartbeat_interval: Duration::from_millis(50),
        ..worker_config()
    };
    let queue = common::open_queue_with_config(&pool, NAMESPACE, QUEUE, &config).await;
    let policy =
        cloudiful_tasks::RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50))
            .unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), config)
        .handler(
            kind(),
            Waiting {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(
        stored.status(),
        TaskStatus::Running,
        "the lease must be left to the recovery"
    );

    // The lease expires, the recovery seals the attempt.
    tokio::time::sleep(Duration::from_millis(600)).await;
    let recovered = queue.recover_expired(10).await.unwrap();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].status(), TaskStatus::Queued);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(
        attempts[0].status(),
        cloudiful_tasks::AttemptStatus::Interrupted
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_worker_drains_the_next_task_without_waiting() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    for _ in 0..3 {
        queue.enqueue(request()).await.unwrap();
    }
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), fast_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(3).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(recorder.handled_count(), 3);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_worker_does_not_consume_the_tasks_of_another_namespace() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let other = open_queue(&pool, "other-ns", QUEUE).await;
    queue.enqueue(request()).await.unwrap();
    other.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    assert_eq!(recorder.handled_count(), 1);
    assert_eq!(task_count(&pool, "other-ns", QUEUE).await, 1);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_worker_survives_a_database_failure() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    // The schema disappears, the claims of the worker fail.
    common::cleanup(&pool).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The database is ready again.
    common::reset(&pool).await;
    let task = queue.enqueue(request()).await.unwrap();
    recorder.wait_for_handled(1).await;

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Succeeded);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_worker_handles_a_task_without_a_notification() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let recorder = Recorder::default();

    let worker = Worker::builder(
        queue,
        WorkerConfig {
            idle_poll_interval: Duration::from_millis(200),
            ..worker_config()
        },
    )
    .handler(
        kind(),
        Recording {
            recorder: recorder.clone(),
        },
    )
    .unwrap()
    .build()
    .unwrap();
    let (shutdown, handle) = start(worker);

    // The task is written without the notification of the queue.
    common::insert_task_without_notification(&pool).await;
    recorder.wait_for_handled(1).await;

    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_worker_reconnects_after_the_listener_breaks() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    // The listener of the worker is connected, its session breaks.
    let backend = common::listening_backend(&pool).await;
    common::terminate_backend(&pool, backend).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let task = queue.enqueue(request()).await.unwrap();
    recorder.wait_for_handled(1).await;

    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Succeeded);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_kind_can_only_have_one_handler() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;

    let error = match Worker::builder(queue, worker_config())
        .handler(
            kind(),
            Recording {
                recorder: Recorder::default(),
            },
        )
        .unwrap()
        .handler(
            kind(),
            Recording {
                recorder: Recorder::default(),
            },
        ) {
        Ok(_) => panic!("a kind can only have one handler"),
        Err(error) => error,
    };

    assert!(
        matches!(error, Error::Validation { .. }),
        "unexpected error: {error}"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_task_without_a_handler_fails_permanently() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            TaskKind::new("other").unwrap(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    tokio::time::sleep(Duration::from_millis(500)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Failed);
    assert_eq!(
        attempt_count(&pool, task.task().id().as_uuid()).await,
        1,
        "a task without a handler must not run again"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_deadline_of_an_attempt_is_enforced_while_another_renewal_is_blocked() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let first = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    // The worker uses its own pool, so that the test can hold the locks of the
    // attempts without starving it.
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .connect_with(common::test_database_options())
        .await
        .unwrap();
    let config = WorkerConfig {
        concurrency: 2,
        lease_ttl: Duration::from_millis(1000),
        heartbeat_interval: Duration::from_millis(200),
        heartbeat_failure_budget: 3,
        cancel_poll_interval: Duration::from_millis(100),
        recovery_interval: Duration::from_millis(200),
        idle_poll_interval: Duration::from_millis(50),
        shutdown_grace: Duration::from_millis(500),
    };
    let worker_queue = Queue::open(worker_pool.clone(), NAMESPACE, QUEUE, &config)
        .await
        .unwrap();
    let worker = Worker::builder(worker_queue, config)
        .handler(
            kind(),
            Ignoring {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    // The second attempt starts later, its deadline lies behind the deadline
    // of the first one.
    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let second = queue.enqueue(request()).await.unwrap();
    recorder.wait_for_handled(2).await;

    // Both attempts wait for a lock, the renewal of an attempt does not renew
    // the other one.
    let mut blocker = pool.begin().await.unwrap();
    for task_id in [first.task().id(), second.task().id()] {
        let attempt_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM cloudiful_tasks.attempts WHERE task_id = $1 ORDER BY attempt_number DESC LIMIT 1",
        )
        .bind(task_id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query("SELECT id FROM cloudiful_tasks.attempts WHERE id = $1 FOR UPDATE")
            .bind(attempt_id)
            .fetch_one(&mut *blocker)
            .await
            .expect("the attempt must be lockable");
    }

    // The deadline of the older attempt fires while the renewal of the newer
    // one is still blocked.
    recorder
        .wait_for_dropped_of(first.task().id().as_uuid())
        .await;
    assert!(
        !recorder.was_dropped(second.task().id().as_uuid()),
        "the newer attempt must still hold its authority"
    );

    blocker.rollback().await.unwrap();
    shutdown.cancel();
    handle.await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_retry_of_a_task_claims_it_again_and_completes_it() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let policy =
        cloudiful_tasks::RetryPolicy::new(2, Duration::from_millis(10), Duration::from_millis(50))
            .unwrap();
    let task = queue
        .enqueue(request().with_retry_policy(policy))
        .await
        .unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            Retrying {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    // The first attempt fails, the task is claimed again and the second
    // attempt completes it.
    recorder.wait_for_handled(2).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(
        stored.status(),
        TaskStatus::Succeeded,
        "the replacement attempt must keep its authority"
    );
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 2);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_pending_claim_does_not_defer_the_shutdown() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    // The worker uses its own pool, the test holds every connection of it, so
    // that the claim of the worker waits for a connection.
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect_with(common::test_database_options())
        .await
        .unwrap();
    let config = WorkerConfig {
        concurrency: 1,
        lease_ttl: Duration::from_secs(10),
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_failure_budget: 3,
        cancel_poll_interval: Duration::from_millis(100),
        recovery_interval: Duration::from_millis(200),
        idle_poll_interval: Duration::from_millis(100),
        shutdown_grace: Duration::from_millis(500),
    };
    let worker_queue = Queue::open(worker_pool.clone(), NAMESPACE, QUEUE, &config)
        .await
        .unwrap();
    let mut busy = Vec::new();
    for _ in 0..2 {
        busy.push(worker_pool.acquire().await.unwrap());
    }
    let worker = Worker::builder(worker_queue, config)
        .handler(
            kind(),
            Recording {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    // The worker reaches its claim and waits for a connection of the pool.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The shutdown must not wait for the pending claim.
    shutdown.cancel();
    let finished = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        finished.is_ok(),
        "the shutdown must not wait for the pending claim"
    );
    finished.unwrap().unwrap().unwrap();

    // The claim that was pending is not dispatched when the pool is released.
    drop(busy);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        recorder.handled_count(),
        0,
        "no handler must start after the shutdown"
    );

    // The task was neither claimed nor finished.
    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(
        stored.status(),
        TaskStatus::Queued,
        "the pending claim must not take the task"
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn the_shutdown_signals_the_running_handlers_while_the_pool_is_busy() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    // The worker uses its own pool, the test holds its connections, so that
    // the claim of the worker waits for a connection.
    let worker_pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(4)
        .connect_with(common::test_database_options())
        .await
        .unwrap();
    let config = WorkerConfig {
        concurrency: 1,
        lease_ttl: Duration::from_secs(10),
        heartbeat_interval: Duration::from_millis(100),
        heartbeat_failure_budget: 3,
        cancel_poll_interval: Duration::from_millis(100),
        recovery_interval: Duration::from_millis(200),
        idle_poll_interval: Duration::from_millis(100),
        shutdown_grace: Duration::from_millis(500),
    };
    let worker_queue = Queue::open(worker_pool.clone(), NAMESPACE, QUEUE, &config)
        .await
        .unwrap();
    let worker = Worker::builder(worker_queue, config)
        .handler(
            kind(),
            Waiting {
                recorder: recorder.clone(),
                work: Duration::from_secs(30),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    // The handler of the task runs.
    recorder.wait_for_handled(1).await;

    // The connections of the worker are busy, its claim waits for one.
    let mut busy = Vec::new();
    while let Ok(Ok(connection)) =
        tokio::time::timeout(Duration::from_millis(200), worker_pool.acquire()).await
    {
        busy.push(connection);
    }

    // The shutdown signals the running handler at once.
    shutdown.cancel();
    recorder.wait_for_cancelled().await;

    let finished = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(
        finished.is_ok(),
        "the shutdown must not wait for the busy pool"
    );
    finished.unwrap().unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn an_external_terminal_skips_the_worker_terminal() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            ExternalCompletion {
                queue: queue.clone(),
                recorder: recorder.clone(),
                result: json!({"external": true}),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Succeeded);
    assert_eq!(
        stored
            .result()
            .and_then(|result| result.get("external").cloned()),
        Some(json!(true))
    );
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Succeeded);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn an_external_failure_skips_the_worker_terminal() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            ExternalFailure {
                queue: queue.clone(),
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Failed);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Failed);
    assert_eq!(
        attempts[0]
            .error()
            .and_then(|error| error.get("reason"))
            .cloned(),
        Some(json!("external failure"))
    );
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn an_external_completion_accepts_the_cancellation_winner() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            ExternalCancellation {
                queue: queue.clone(),
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Cancelled);
    assert!(stored.result().is_none());
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Cancelled);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn a_newer_attempt_terminal_does_not_authorize_the_external_completion() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            NewerTerminal {
                pool: pool.clone(),
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(
        stored.status(),
        TaskStatus::Failed,
        "the newer terminal must not authorize the captured attempt"
    );
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    let first = attempts
        .iter()
        .find(|attempt| attempt.attempt_number() == 1)
        .expect("the captured attempt must exist");
    let newer = attempts
        .iter()
        .find(|attempt| attempt.attempt_number() == 2)
        .expect("the newer attempt must exist");
    assert_eq!(first.status(), AttemptStatus::Failed);
    assert_eq!(newer.status(), AttemptStatus::Succeeded);
}

#[tokio::test]
#[ignore = "requires CLOUDIFUL_TASKS_TEST_DATABASE_URL"]
async fn an_unsealed_external_terminal_fails_the_attempt() {
    let pool = connect().await;
    reset(&pool).await;
    let queue = open_queue(&pool, NAMESPACE, QUEUE).await;
    let task = queue.enqueue(request()).await.unwrap();
    let recorder = Recorder::default();

    let worker = Worker::builder(queue.clone(), worker_config())
        .handler(
            kind(),
            UnsealedExternalCompletion {
                recorder: recorder.clone(),
            },
        )
        .unwrap()
        .build()
        .unwrap();
    let (shutdown, handle) = start(worker);

    recorder.wait_for_handled(1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    shutdown.cancel();
    handle.await.unwrap().unwrap();

    let stored = queue.get(task.task().id()).await.unwrap().unwrap();
    assert_eq!(stored.status(), TaskStatus::Failed);
    let attempts = queue.attempts(task.task().id(), 10).await.unwrap();
    assert_eq!(attempts.len(), 1);
    assert_eq!(attempts[0].status(), AttemptStatus::Failed);
    assert_eq!(
        attempts[0]
            .error()
            .and_then(|error| error.get("reason"))
            .cloned(),
        Some(json!("external completion was not sealed"))
    );
}
