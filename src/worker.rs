//! The worker runtime.
//!
//! A worker claims the tasks of one queue and runs their handlers. Nothing
//! starts on its own: the worker runs when [`Worker::run`] is called and it
//! stops when the shutdown token of the caller is cancelled.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::task::{AbortHandle, JoinError, JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::config::WorkerConfig;
use crate::error::{Error, Result};
use crate::handler::{HandlerError, Outcome, SharedHandler, TaskContext, TaskHandler};
use crate::model::{Attempt, Failure, TaskId, TaskKind};
use crate::queue::Queue;
use crate::wake::{WakeListener, Wakeup};

/// The delay before the worker tries again after a failed claim.
const CLAIM_BACKOFF: Duration = Duration::from_millis(100);

/// The number of tasks a recovery pass handles.
const RECOVERY_LIMIT: u32 = 1000;

/// The identity of the attempt an action of the registry belongs to.
///
/// An action of the registry only changes the entry of the attempt it was
/// started for. An attempt that was replaced by a later attempt of the same
/// task keeps its own state: a callback of the replaced attempt changes
/// nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AttemptIdentity {
    id: Uuid,
    fencing_token: Uuid,
}

impl AttemptIdentity {
    fn of(attempt: &Attempt) -> Self {
        Self {
            id: attempt.id().as_uuid(),
            fencing_token: attempt.fencing_token().as_uuid(),
        }
    }

    fn matches(&self, attempt: &Attempt) -> bool {
        self.id == attempt.id().as_uuid() && self.fencing_token == attempt.fencing_token().as_uuid()
    }
}

/// A running attempt of the worker.
///
/// The entry holds the local authority of the attempt: the deadline of its
/// lease and the handle that stops its handler. When the entry is gone, the
/// worker gave the authority up and no write of the attempt is allowed.
struct ActiveAttempt {
    attempt: Attempt,
    cancel: CancellationToken,
    abort: Option<AbortHandle>,
    deadline: Instant,
    failures: u32,
    renewing: bool,
    terminal: bool,
}

/// The registry of the running attempts of a worker.
///
/// Every action of the registry belongs to the attempt it was started for. A
/// callback of an attempt that was replaced by a later attempt of the same
/// task changes nothing.
#[derive(Default)]
struct Registry {
    active: Mutex<HashMap<TaskId, ActiveAttempt>>,
}

impl Registry {
    fn register(
        &self,
        task_id: TaskId,
        attempt: Attempt,
        cancel: CancellationToken,
        abort: Option<AbortHandle>,
        deadline: Instant,
    ) {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        active.insert(
            task_id,
            ActiveAttempt {
                attempt,
                cancel,
                abort,
                deadline,
                failures: 0,
                renewing: false,
                terminal: false,
            },
        );
    }

    /// Takes the authority of an attempt out of the registry.
    ///
    /// The authority is only valid while the entry of the attempt is there.
    fn take(&self, task_id: &TaskId, identity: AttemptIdentity) -> Option<ActiveAttempt> {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        match active.get(task_id) {
            Some(entry) if identity.matches(&entry.attempt) => active.remove(task_id),
            _ => None,
        }
    }

    /// Gives the authority of an attempt up.
    ///
    /// The handler of the attempt is stopped, also when it ignores the
    /// cancellation token of its attempt.
    fn revoke(&self, task_id: &TaskId, identity: AttemptIdentity) {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        let matches = active
            .get(task_id)
            .is_some_and(|entry| identity.matches(&entry.attempt) && !entry.terminal);
        if !matches {
            return;
        }
        if let Some(entry) = active.remove(task_id) {
            entry.cancel.cancel();
            if let Some(abort) = entry.abort {
                abort.abort();
            }
        }
    }

    /// Cancels the token of an attempt.
    fn cancel(&self, task_id: &TaskId, identity: AttemptIdentity) {
        let active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        if let Some(entry) = active.get(task_id)
            && identity.matches(&entry.attempt)
            && !entry.terminal
        {
            entry.cancel.cancel();
        }
    }

    fn cancel_all(&self) {
        let active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        for attempt in active.values() {
            attempt.cancel.cancel();
        }
    }

    /// The attempts that still hold their authority.
    fn active_attempts(&self) -> Vec<(TaskId, Attempt, Instant)> {
        let active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        active
            .values()
            .filter(|entry| !entry.terminal)
            .map(|entry| {
                (
                    entry.attempt.task_id(),
                    entry.attempt.clone(),
                    entry.deadline,
                )
            })
            .collect()
    }

    /// Marks the renewal of an attempt as running.
    ///
    /// The renewal of an attempt runs at most once at a time.
    fn start_renewal(&self, task_id: &TaskId, identity: AttemptIdentity) -> bool {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        match active.get_mut(task_id) {
            Some(entry)
                if identity.matches(&entry.attempt) && !entry.renewing && !entry.terminal =>
            {
                entry.renewing = true;
                true
            }
            _ => false,
        }
    }

    fn mark_terminal(&self, task_id: &TaskId, identity: AttemptIdentity) {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        if let Some(entry) = active.get_mut(task_id)
            && identity.matches(&entry.attempt)
        {
            entry.terminal = true;
            entry.renewing = false;
        }
    }

    /// Marks the renewal of an attempt as finished.
    fn finish_renewal(&self, task_id: &TaskId, identity: AttemptIdentity) {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        if let Some(entry) = active.get_mut(task_id)
            && identity.matches(&entry.attempt)
        {
            entry.renewing = false;
        }
    }

    /// Whether the renewal of an attempt runs.
    #[cfg(test)]
    fn is_renewing(&self, task_id: &TaskId, identity: AttemptIdentity) -> bool {
        let active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        match active.get(task_id) {
            Some(entry) if identity.matches(&entry.attempt) => entry.renewing,
            _ => false,
        }
    }

    /// The number of the failed renewals of an attempt.
    #[cfg(test)]
    fn failures(&self, task_id: &TaskId, identity: AttemptIdentity) -> u32 {
        let active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        match active.get(task_id) {
            Some(entry) if identity.matches(&entry.attempt) => entry.failures,
            _ => 0,
        }
    }

    /// Counts a failed renewal of an attempt.
    fn count_failure(&self, task_id: &TaskId, identity: AttemptIdentity) -> u32 {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        match active.get_mut(task_id) {
            Some(entry) if identity.matches(&entry.attempt) => {
                entry.failures += 1;
                entry.failures
            }
            _ => 0,
        }
    }

    /// Moves the deadline of an attempt after a renewal.
    fn extend_deadline(&self, task_id: &TaskId, identity: AttemptIdentity, deadline: Instant) {
        let mut active = self
            .active
            .lock()
            .expect("the registry of the worker is not poisoned");
        if let Some(entry) = active.get_mut(task_id)
            && identity.matches(&entry.attempt)
        {
            entry.deadline = deadline;
            entry.failures = 0;
        }
    }
}

/// The shared state of a running worker.
struct RunContext {
    queue: Queue,
    config: WorkerConfig,
    worker_id: String,
    handlers: HashMap<TaskKind, SharedHandler>,
    registry: Registry,
    wakeup: Wakeup,
    shutdown: CancellationToken,
}

/// Whether a claimed task may still be started.
///
/// A task that was claimed while the shutdown was requested is not started.
/// Its lease stays recoverable, the task is never finished with a result the
/// worker did not produce.
fn may_dispatch(shutdown: &CancellationToken) -> bool {
    !shutdown.is_cancelled()
}

/// The margin between the local deadline of a lease and its real expiry.
///
/// The local deadline is a conservative estimate of the expiry of the lease,
/// the margin keeps the worker from starting a renewal that would finish
/// after the lease expired.
fn authority_margin(lease_ttl: Duration) -> Duration {
    (lease_ttl / 10).max(Duration::from_millis(1))
}

/// A worker of one queue.
///
/// The worker holds the handlers of the kinds it serves. A kind without a
/// handler fails permanently, it is never retried in a loop.
pub struct Worker {
    queue: Queue,
    config: WorkerConfig,
    worker_id: String,
    handlers: HashMap<TaskKind, SharedHandler>,
}

impl Worker {
    /// Builds a worker for a queue.
    pub fn builder(queue: Queue, config: WorkerConfig) -> WorkerBuilder {
        WorkerBuilder {
            queue,
            config,
            worker_id: format!("worker-{}", uuid::Uuid::new_v4()),
            handlers: HashMap::new(),
        }
    }

    /// The queue of the worker.
    pub fn queue(&self) -> &Queue {
        &self.queue
    }

    /// The configuration of the worker.
    pub fn config(&self) -> &WorkerConfig {
        &self.config
    }

    /// Runs the worker until the shutdown is requested.
    ///
    /// The worker stops claiming new tasks, asks its handlers to stop and
    /// waits for the grace period of the configuration. Handlers that do not
    /// stop in time are aborted, their leases stay recoverable.
    pub async fn run(self, shutdown: CancellationToken) -> Result<()> {
        // The lease of the queue decides the lifetime of a lease, the
        // intervals of the worker are checked against it.
        let mut config = self.config.clone();
        config.lease_ttl = self.queue.lease_ttl();
        config.validate()?;
        let context = Arc::new(RunContext {
            queue: self.queue.clone(),
            config,
            worker_id: self.worker_id.clone(),
            handlers: self.handlers.clone(),
            registry: Registry::default(),
            wakeup: Wakeup::new(),
            shutdown: shutdown.clone(),
        });

        let wakeup = context.wakeup.clone();
        let listener = tokio::spawn(
            WakeListener::new(context.queue.pool().clone()).run(wakeup, shutdown.clone()),
        );
        let heartbeat = tokio::spawn(heartbeat_loop(context.clone()));
        let recovery = tokio::spawn(recovery_loop(context.clone()));
        let cancellation = tokio::spawn(cancellation_loop(context.clone()));

        let result = claim_loop(context.clone()).await;

        listener.abort();
        heartbeat.abort();
        recovery.abort();
        cancellation.abort();

        result
    }
}

/// The builder of a worker.
pub struct WorkerBuilder {
    queue: Queue,
    config: WorkerConfig,
    worker_id: String,
    handlers: HashMap<TaskKind, SharedHandler>,
}

impl WorkerBuilder {
    /// Registers the handler of a kind.
    ///
    /// A kind can only be registered once.
    pub fn handler(mut self, kind: TaskKind, handler: impl TaskHandler + 'static) -> Result<Self> {
        if self.handlers.contains_key(&kind) {
            return Err(Error::Validation {
                field: "kind",
                reason: format!("the kind `{kind}` already has a handler"),
            });
        }
        self.handlers.insert(kind, Arc::new(handler));
        Ok(self)
    }

    /// Sets the identifier of the worker.
    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Result<Self> {
        let worker_id = worker_id.into();
        if worker_id.is_empty() || worker_id.len() > 128 {
            return Err(Error::Validation {
                field: "worker_id",
                reason: "must be between 1 and 128 bytes".to_owned(),
            });
        }
        self.worker_id = worker_id;
        Ok(self)
    }

    /// Builds the worker.
    pub fn build(self) -> Result<Worker> {
        self.config.validate()?;
        Ok(Worker {
            queue: self.queue,
            config: self.config,
            worker_id: self.worker_id,
            handlers: self.handlers,
        })
    }
}

/// Claims the tasks of the queue and runs their handlers.
async fn claim_loop(context: Arc<RunContext>) -> Result<()> {
    let mut handlers = JoinSet::new();
    let permits = Arc::new(tokio::sync::Semaphore::new(context.config.concurrency));

    loop {
        if context.shutdown.is_cancelled() {
            break;
        }
        while let Some(result) = handlers.try_join_next() {
            let _ = result;
        }

        let permit = tokio::select! {
            _ = context.shutdown.cancelled() => break,
            permit = permits.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };

        // The claim waits for a connection and for the database. The shutdown
        // interrupts it, so that the grace period of the worker starts at once
        // and is not deferred by a busy pool or by a slow statement.
        let claim = tokio::select! {
            biased;
            _ = context.shutdown.cancelled() => break,
            claim = context.queue.claim(&context.worker_id) => claim,
        };

        match claim {
            Ok(Some(claim)) => {
                // The dispatch boundary: a task that was claimed while the
                // shutdown was requested is not started. Its lease stays
                // recoverable, the task is not finished.
                if !may_dispatch(&context.shutdown) {
                    drop(permit);
                    break;
                }
                let task_context = TaskContext::new(
                    context.queue.clone(),
                    claim.attempt().clone(),
                    claim.task().id(),
                    claim.task().kind().clone(),
                    claim.task().payload().clone(),
                    CancellationToken::new(),
                );
                let deadline = Instant::now() + context.config.lease_ttl
                    - authority_margin(context.config.lease_ttl);
                let handler = context.handlers.get(claim.task().kind()).cloned();
                let task_context = Arc::new(task_context);
                let context = context.clone();
                handlers.spawn(async move {
                    supervise(context, handler, task_context, permit, deadline).await;
                });
            }
            Ok(None) => {
                drop(permit);
                tokio::select! {
                    _ = context.shutdown.cancelled() => break,
                    _ = context.wakeup.wait() => {}
                    _ = tokio::time::sleep(context.config.idle_poll_interval) => {}
                }
            }
            Err(_) => {
                drop(permit);
                tokio::select! {
                    _ = context.shutdown.cancelled() => break,
                    _ = tokio::time::sleep(CLAIM_BACKOFF) => {}
                }
            }
        }
    }

    shutdown(&context, &mut handlers).await;
    Ok(())
}

/// Runs a handler and writes its outcome.
async fn supervise(
    context: Arc<RunContext>,
    handler: Option<SharedHandler>,
    task_context: Arc<TaskContext>,
    _permit: tokio::sync::OwnedSemaphorePermit,
    deadline: Instant,
) {
    let task_id = task_context.task_id();
    let attempt = task_context.attempt().clone();
    let identity = AttemptIdentity::of(&attempt);
    let cancel = task_context.cancellation_token();

    let outcome = match handler {
        Some(handler) => {
            let handle = tokio::spawn(async move { handler.handle((*task_context).clone()).await });
            let abort = handle.abort_handle();
            context.registry.register(
                task_id,
                attempt.clone(),
                cancel.clone(),
                Some(abort),
                deadline,
            );
            let handle = HandlerTask { handle };
            match handle.wait().await {
                Ok(Ok(outcome)) => outcome,
                Ok(Err(error)) => Outcome::failed(Failure::permanent(Some(failure_payload(
                    &error.to_string(),
                )))),
                Err(join_error) => {
                    if join_error.is_panic() {
                        Outcome::failed(Failure::permanent(Some(failure_payload(
                            "handler panicked",
                        ))))
                    } else {
                        Outcome::failed(Failure::permanent(Some(failure_payload(
                            "handler was aborted",
                        ))))
                    }
                }
            }
        }
        None => {
            context
                .registry
                .register(task_id, attempt.clone(), cancel.clone(), None, deadline);
            Outcome::failed(Failure::permanent(Some(failure_payload(
                "no handler for the kind",
            ))))
        }
    };

    // The authority of the attempt must still be there. When the worker gave
    // it up, the attempt lost its lease and no write is allowed, also when the
    // lease of the database is still live.
    let Some(active) = context.registry.take(&task_id, identity) else {
        return;
    };
    if active.terminal {
        context.wakeup.wake();
        return;
    }

    if context.shutdown.is_cancelled() {
        // The worker stops here. The lease stays recoverable, the task is not
        // finished with an outcome the worker did not produce.
        return;
    }

    let result = match outcome {
        Outcome::Completed(result) => context.queue.complete(&attempt, result).await.map(|_| ()),
        Outcome::Failed(failure) => context.queue.fail(&attempt, &failure).await.map(|_| ()),
        Outcome::ExternallyCompleted => {
            match context.queue.attempt_is_sealed(&attempt).await {
                // The handler committed the terminal of this exact attempt.
                // The worker must not write a second terminal.
                Ok(true) => Ok(()),
                // The signal is not backed by a seal of this attempt. The
                // normal failure write either seals the still owned attempt or
                // reports the lost lease without touching a newer attempt.
                Ok(false) => context
                    .queue
                    .fail(
                        &attempt,
                        &Failure::permanent(Some(failure_payload(
                            "external completion was not sealed",
                        ))),
                    )
                    .await
                    .map(|_| ()),
                Err(error) => Err(error),
            }
        }
    };
    let _ = result;

    context.wakeup.wake();
}

/// The payload of a failure of the worker.
fn failure_payload(reason: &str) -> Value {
    serde_json::json!({ "reason": reason })
}

/// A spawned handler that is aborted when it is dropped.
///
/// The handle stays in the struct while the handler is awaited, so that the
/// handler is aborted when the worker drops it, also when the handler ignores
/// the cancellation token of its attempt.
struct HandlerTask {
    handle: JoinHandle<std::result::Result<Outcome, HandlerError>>,
}

impl HandlerTask {
    async fn wait(
        mut self,
    ) -> std::result::Result<std::result::Result<Outcome, HandlerError>, JoinError> {
        (&mut self.handle).await
    }
}

impl Drop for HandlerTask {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Renews the leases of the running attempts.
///
/// Every attempt is renewed on a task of its own, so that a renewal that
/// waits for a connection or for the row of an attempt cannot defer the
/// deadline of another attempt.
async fn heartbeat_loop(context: Arc<RunContext>) {
    let mut ticker = tokio::time::interval(context.config.heartbeat_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut renewals = JoinSet::new();
    loop {
        tokio::select! {
            _ = context.shutdown.cancelled() => break,
            _ = ticker.tick() => {}
        }
        while renewals.try_join_next().is_some() {}
        for (task_id, attempt, deadline) in context.registry.active_attempts() {
            let identity = AttemptIdentity::of(&attempt);
            if !context.registry.start_renewal(&task_id, identity) {
                continue;
            }
            let context = context.clone();
            renewals.spawn(async move {
                renew_attempt(context, task_id, attempt, deadline).await;
            });
        }
    }
    renewals.abort_all();
}

/// Renews the lease of one attempt.
///
/// The renewal is bounded by the authority that is left. It must not wait past
/// the expiry of the lease, not even when the pool or the row of the attempt
/// is busy.
async fn renew_attempt(
    context: Arc<RunContext>,
    task_id: TaskId,
    attempt: Attempt,
    deadline: Instant,
) {
    let identity = AttemptIdentity::of(&attempt);
    let margin = authority_margin(context.config.lease_ttl);
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        revoke_unless_sealed(&context, task_id, &attempt, margin).await;
        return;
    }
    let budget = remaining.saturating_sub(margin);
    match tokio::time::timeout(budget, context.queue.renew(&attempt)).await {
        Ok(Ok(_)) => {
            let deadline = Instant::now() + context.config.lease_ttl - margin;
            context
                .registry
                .extend_deadline(&task_id, identity, deadline);
        }
        Ok(Err(error)) => {
            let lost = matches!(error, Error::LeaseLost { .. });
            if lost {
                match sealed_within(&context, &attempt, margin).await {
                    Some(true) => context.registry.mark_terminal(&task_id, identity),
                    Some(false) => context.registry.revoke(&task_id, identity),
                    None => {
                        let failures = context.registry.count_failure(&task_id, identity);
                        if failures > context.config.heartbeat_failure_budget
                            || Instant::now() >= deadline
                        {
                            context.registry.revoke(&task_id, identity);
                        }
                    }
                }
            } else {
                let failures = context.registry.count_failure(&task_id, identity);
                if failures > context.config.heartbeat_failure_budget {
                    revoke_unless_sealed(&context, task_id, &attempt, margin).await;
                }
            }
        }
        Err(_elapsed) => {
            revoke_unless_sealed(&context, task_id, &attempt, margin).await;
        }
    }
    context.registry.finish_renewal(&task_id, identity);
}

async fn sealed_within(context: &RunContext, attempt: &Attempt, budget: Duration) -> Option<bool> {
    tokio::time::timeout(budget, context.queue.attempt_is_sealed(attempt))
        .await
        .ok()
        .and_then(std::result::Result::ok)
}

async fn revoke_unless_sealed(
    context: &RunContext,
    task_id: TaskId,
    attempt: &Attempt,
    budget: Duration,
) {
    let identity = AttemptIdentity::of(attempt);
    // The final probe uses only the reserved authority margin. An unavailable
    // database never grants an unbounded continuation to an unverified attempt.
    if sealed_within(context, attempt, budget).await == Some(true) {
        context.registry.mark_terminal(&task_id, identity);
    } else {
        context.registry.revoke(&task_id, identity);
    }
}

/// Asks the running attempts to stop when their task was cancelled.
async fn cancellation_loop(context: Arc<RunContext>) {
    let mut ticker = tokio::time::interval(context.config.cancel_poll_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = context.shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        for (task_id, attempt, _) in context.registry.active_attempts() {
            match context.queue.get(task_id).await {
                Ok(Some(task)) => {
                    if task.status() == crate::model::TaskStatus::CancelRequested {
                        let identity = AttemptIdentity::of(&attempt);
                        context.registry.cancel(&task_id, identity);
                    }
                }
                Ok(None) => {}
                Err(_) => {}
            }
        }
    }
}

/// Seals the attempts whose lease expired.
async fn recovery_loop(context: Arc<RunContext>) {
    let mut ticker = tokio::time::interval(context.config.recovery_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = context.shutdown.cancelled() => return,
            _ = ticker.tick() => {}
        }
        let _ = context.queue.recover_expired(RECOVERY_LIMIT).await;
    }
}

/// Stops the running handlers when the worker shuts down.
async fn shutdown(context: &Arc<RunContext>, handlers: &mut JoinSet<()>) {
    context.registry.cancel_all();

    let deadline = Instant::now() + context.config.shutdown_grace;
    while !handlers.is_empty() {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            result = handlers.join_next() => {
                let _ = result;
            }
        }
    }

    handlers.abort_all();
    while handlers.join_next().await.is_some() {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{AttemptId, AttemptStatus, FencingToken, TaskId};

    fn attempt(task_id: TaskId, number: u32) -> Attempt {
        Attempt {
            id: AttemptId::new(),
            task_id,
            attempt_number: number,
            worker_id: "worker-1".to_owned(),
            fencing_token: FencingToken::new(),
            status: AttemptStatus::Running,
            lease_expires_at: None,
            started_at: chrono::DateTime::UNIX_EPOCH,
            finished_at: None,
            error: None,
            progress: None,
            progress_updated_at: None,
        }
    }

    #[test]
    fn a_callback_of_a_replaced_attempt_changes_nothing() {
        let registry = Registry::default();
        let task_id = TaskId::new();
        let token = CancellationToken::new();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("the runtime must build");
        let handler = runtime.spawn(std::future::pending::<()>());

        // The first attempt runs and is registered.
        let first = attempt(task_id, 1);
        let first_identity = AttemptIdentity::of(&first);
        registry.register(task_id, first.clone(), token.clone(), None, Instant::now());
        assert!(registry.start_renewal(&task_id, first_identity));

        // The supervisor of the first attempt takes its authority out, the
        // task is retried and a later attempt of the same task is registered.
        assert!(registry.take(&task_id, first_identity).is_some());
        let second = attempt(task_id, 2);
        let second_identity = AttemptIdentity::of(&second);
        registry.register(
            task_id,
            second.clone(),
            token.clone(),
            Some(handler.abort_handle()),
            Instant::now(),
        );

        // The second attempt starts its own renewal and counts a failure.
        assert!(registry.start_renewal(&task_id, second_identity));
        assert_eq!(registry.count_failure(&task_id, second_identity), 1);

        // The callbacks of the first attempt change nothing of the second one.
        let deadline = Instant::now() + Duration::from_secs(30);
        registry.mark_terminal(&task_id, first_identity);
        registry.revoke(&task_id, first_identity);
        registry.extend_deadline(&task_id, first_identity, deadline);
        registry.finish_renewal(&task_id, first_identity);
        assert_eq!(registry.count_failure(&task_id, first_identity), 0);
        assert!(registry.take(&task_id, first_identity).is_none());

        // The entry of the second attempt is untouched: its renewal still
        // runs, its failures and its deadline are unchanged and its handler
        // was not stopped.
        assert!(!token.is_cancelled());
        assert!(registry.is_renewing(&task_id, second_identity));
        assert_eq!(registry.failures(&task_id, second_identity), 1);
        let (_, entry_attempt, entry_deadline) = registry
            .active_attempts()
            .pop()
            .expect("the second attempt must hold its authority");
        assert_eq!(entry_attempt.id(), second.id());
        assert_ne!(entry_deadline, deadline);

        // The second attempt renews on its own.
        registry.extend_deadline(&task_id, second_identity, deadline);
        let (_, _, entry_deadline) = registry
            .active_attempts()
            .pop()
            .expect("the second attempt must hold its authority");
        assert_eq!(entry_deadline, deadline);
        registry.mark_terminal(&task_id, second_identity);
        assert!(registry.active_attempts().is_empty());
        assert!(!registry.start_renewal(&task_id, second_identity));
        registry.cancel(&task_id, second_identity);
        registry.revoke(&task_id, second_identity);
        assert!(!token.is_cancelled());
        registry.cancel_all();
        assert!(token.is_cancelled());
        assert!(registry.take(&task_id, second_identity).unwrap().terminal);
    }

    #[test]
    fn a_claimed_task_is_not_started_after_the_shutdown() {
        let shutdown = CancellationToken::new();
        assert!(
            may_dispatch(&shutdown),
            "a running worker dispatches a task"
        );

        shutdown.cancel();
        assert!(
            !may_dispatch(&shutdown),
            "a task that was claimed after the shutdown must not be started"
        );
    }

    #[test]
    fn the_renewal_of_a_replaced_attempt_does_not_count_a_failure() {
        let registry = Registry::default();
        let task_id = TaskId::new();
        let token = CancellationToken::new();

        let first = attempt(task_id, 1);
        let first_identity = AttemptIdentity::of(&first);
        registry.register(task_id, first.clone(), token.clone(), None, Instant::now());

        let second = attempt(task_id, 2);
        let second_identity = AttemptIdentity::of(&second);
        registry.register(task_id, second.clone(), token.clone(), None, Instant::now());

        // The failure of the first attempt is not counted for the second one.
        assert_eq!(registry.count_failure(&task_id, first_identity), 0);
        assert_eq!(registry.count_failure(&task_id, second_identity), 1);
        assert_eq!(registry.count_failure(&task_id, first_identity), 0);
    }
}
