# cloudiful-tasks

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

[English](README.md) | [简体中文](README.zh-CN.md)

Durable task execution on PostgreSQL.

`cloudiful-tasks` keeps tasks, their attempts and their leases in one PostgreSQL
schema that the library owns. Nothing runs on its own: a worker runtime is
started explicitly by the application.

Execution is **at least once**. A worker that loses its lease can still finish
its work, so handlers must be idempotent and side effects must be fenced with
the fencing token of the attempt.

## Capabilities

- the data model: namespaces, queues, task kinds, task and attempt states, retry
  policies, progress reports and fencing tokens,
- an explicit schema bootstrap with an ownership marker, a versioned schema
  protocol and migration checksum verification,
- the durable queue store: enqueue with idempotency, claim with a lease,
  renewal, progress, completion, failure, cancellation, manual retry,
  cooperative per-task pause and resume, release of an owned attempt, and
  recovery of expired leases,
- scoped deletion of a terminal task, bounded age pruning and an opt-in
  retention runner for terminal tasks,
- durable queue policies with an optional limit on running attempts,
- the worker runtime: bounded concurrency, lease heartbeat, cooperative
  cancellation, recovery of expired leases, wake up notifications, a
  handler-owned terminal outcome and a graceful shutdown.

## Install

The crate is not published to crates.io. Use the Git dependency and pin the
commit you verified:

```toml
[dependencies]
cloudiful-tasks = { git = "https://github.com/cloudiful/tasks", rev = "<COMMIT_SHA>" }
sea-orm = { version = "2.0.4", default-features = false, features = [
    "macros",
    "with-json",
    "with-uuid",
    "with-time",
    "sqlx-postgres",
    "runtime-tokio-rustls",
] }
tokio-util = "0.7"
```

The public API takes a `sea_orm::DatabaseConnection`, so the application supplies
the connection and its pool configuration. Timestamps are `time::OffsetDateTime`
and must be representable in UTC within the default `time` year range (-9999
through 9999); PostgreSQL values outside that range are not supported. A
persisted availability timestamp uses UTC with exactly six fractional digits and
a `Z` suffix, so the same request keeps the same identity across builds.

## Usage

### The schema

The schema is created explicitly. Opening a queue or starting a worker never
creates or alters schema.

```rust
use cloudiful_tasks::{check_schema, migrate};
use sea_orm::Database;

let db = Database::connect("postgresql://user@localhost/tasks").await?;
migrate(&db).await?;
check_schema(&db).await?;
```

`migrate` is idempotent and serialized per database: it creates a missing schema
and upgrades a supported older version. `check_schema` only reads and reports
`SchemaMissing` or `OutdatedSchemaVersion` instead. Both refuse foreign schemas
(`ForeignSchema`) and newer protocols (`UnsupportedSchemaVersion`); a schema
that the library does not own is never adopted or overwritten. A ledger row that
did not complete successfully is a dirty migration: it is reported as
`DirtyMigration` and never retried.

The schema protocol version 5 adds the partial index of the terminal cleanup.
The upgrade is additive and keeps every task, attempt, payload and lease. A
binary of an older version refuses a schema of a newer protocol version, so a
rollout stops the old writers, migrates with the new version and starts the new
writers. Mixed protocol versions are not supported and a rollback after the new
version wrote is not promised. A consumer that stays pinned to an older protocol
keeps its old behavior; it is not upgraded by this schema change.

### The queue

A queue addresses the tasks of one namespace and one queue name.

```rust
use cloudiful_tasks::{EnqueueRequest, Queue, TaskKind, WorkerConfig};
use serde_json::json;

let queue = Queue::open(db.clone(), "stock", "k8s", &WorkerConfig::default()).await?;
let request = EnqueueRequest::new(TaskKind::new("sync")?, json!({"cluster": "a"})).with_idempotency_key("sync:cluster-a")?;
queue.enqueue(request).await?;

if let Some(claim) = queue.claim("worker-1").await? {
    queue.renew(claim.attempt()).await?;
    queue.complete(claim.attempt(), Some(json!({"ok": true}))).await?;
}
```

- The same idempotency key with the same request returns the existing task,
  also when it already finished; a different request with the same key is an
  `IdempotencyConflict`. A request without a key always creates a task.
- Only a ready task is claimed, highest priority first. The claim holds the
  lease and a fresh fencing token; a renewal, a progress report, a completion
  and a failure need the attempt, its token and an unexpired lease, otherwise
  the call reports `LeaseLost` and changes nothing.
- An expired attempt is sealed as interrupted and its task is scheduled again
  while the budget allows it. Retryable failures are retried automatically within
  the remaining budget; `retry_failed` manually retries a terminal failed task.
  A cancellation that was requested before the completion wins.
- `claim_by_id` claims exactly the requested task. A queue policy, set with
  `queue.set_policy(QueuePolicy { max_concurrent_attempts: Some(1) })`, limits
  the running attempts of a queue for every instance that opens it.

### The worker

Nothing starts before `run` is called.

```rust
use async_trait::async_trait;
use cloudiful_tasks::{CancellationToken, HandlerError, Outcome, TaskContext, TaskHandler, TaskKind, Worker, WorkerConfig};
use serde_json::json;

struct Sync;

#[async_trait]
impl TaskHandler for Sync {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        Ok(Outcome::completed(Some(json!({"synced": true}))))
    }
}

let config = WorkerConfig::default();
let queue = Queue::open(db.clone(), "stock", "k8s", &config).await?;
let worker = Worker::builder(queue, config).handler(TaskKind::new("sync")?, Sync)?.build()?;
worker.run(CancellationToken::new()).await?;
```

- The number of running handlers is bounded by the configuration. The worker
  renews the lease of its running attempts and stops a handler whose lease was
  lost, so such a handler cannot write.
- A cancelled task stops its handler, a panic fails the task permanently and the
  worker keeps running. The payload of a task is never printed.
- `Outcome::ExternallyCompleted` tells the worker that the handler already
  committed the terminal of its attempt; the worker verifies the attempt, the
  fencing token and the sealed status before it skips its own terminal write.
  On shutdown it stops claiming, asks the handlers to stop and waits for the
  grace period.

### Cooperative pause

A pause is scoped to one task and is independent of the task status, so a queued
task can be paused while it stays queued.

```rust
let paused = queue.request_pause(task_id).await?;
if paused.pause_state() == cloudiful_tasks::TaskPauseState::Pausing {
    // A running attempt parks at its next checkpoint.
}
queue.resume(task_id).await?;
```

A running task keeps its lease while it pauses. The handler observes
`TaskContext::pause_requested`, persists its checkpoint and returns
`Outcome::Paused`, and the worker seals the exact attempt with `park_paused`. A
resume queues the paused task again, keeping its available time, its attempt
history and its effective failure budget. A pause of one task never stops
another task.

### Releasing an owned attempt

A dispatcher that stops a worker can hand the task of that worker back to the
queue instead of waiting for the lease to expire.

```rust
let claim = queue.claim("worker-1").await?.expect("a task");
// The work of the attempt is stopped and joined by the caller.
queue.release(claim.attempt(), "worker-1").await?;
```

`release` seals the exact attempt as `released` and queues its task again
immediately, without a backoff. The attempt keeps its place in the attempt
history, and the release does not consume the failure budget of the task: the
effective budget of a task is `attempt_count - paused_attempts -
released_attempts`, so a task with `max_attempts` of 1 can still run once after
a release.

The second argument is the identity of the caller that stops the work. It is
supplied by the application and matched against the worker of the attempt
independently, so the attempt of another worker is never released, also when
that attempt was read from the queue. The identity is an execution identity of
the application, not an authentication boundary.

The release needs the attempt, its fencing token, an unexpired lease and the
owning identity. A wrong owner, a stale token, an expired lease and an attempt
that a newer attempt replaced are refused with `LeaseLost` and change nothing.
A release that was already applied by the same owner is a read only replay: it
returns the current task and changes nothing, also when the task runs a newer
attempt by then. A cancellation that was requested before the release wins and
ends the task as cancelled; a task that waits for a cooperative pause stays
paused and unclaimable. A genuine release notifies the workers, also when the
task stays paused; a replay never notifies. The library never releases an
attempt of a running worker: the caller must stop and join the work first.

### Terminal cleanup and opt-in retention

A terminal task can be deleted explicitly, or an application can run a bounded
retention loop over the terminal tasks of a queue.

```rust
let deleted = queue.delete_terminal(task_id).await?;
let pruned = queue
    .prune_terminal(TaskStatus::Succeeded, Duration::from_secs(7 * 24 * 60 * 60), 100)
    .await?;
```

`delete_terminal` deletes exactly one terminal task of the queue and reports
`false` for an absent or out-of-scope task. A task that is still active is
refused with `InvalidTaskState`. `prune_terminal` deletes the oldest tasks of
one terminal status whose `finished_at` is older than the given age, up to the
limit of 1 through 1000. Both operations lock the task rows, so they never
delete a task that a concurrent retry or claim reactivated.

A retention loop is started explicitly and is the only automatic cleanup entry
point:

```rust
let config = cloudiful_tasks::RetentionConfig::new(Duration::from_secs(60 * 60), 100)?
    .with_status(TaskStatus::Succeeded, Duration::from_secs(7 * 24 * 60 * 60))?
    .with_status(TaskStatus::Failed, Duration::from_secs(30 * 24 * 60 * 60))?;

let shutdown = CancellationToken::new();
let runner = tokio::spawn({
    let queue = queue.clone();
    async move { queue.run_retention(config, shutdown.clone()).await }
});
// Later: shutdown.cancel(); and join the runner.
```

Nothing runs before the returned future is polled, and the first cycle waits a
full interval. A cycle deletes at most `max_per_cycle` tasks in total and
chooses the globally oldest eligible tasks across the enabled statuses, so one
status never spends the whole budget while another is old enough for deletion.
A status without a configured age is never deleted. Cancellation is observed
between cycles: the running cycle finishes its transaction and the call returns
`Ok(())`. An error of a cycle is returned to the caller, which owns the task,
supervision and logging. No retention starts on migration, on opening a queue
or with a worker.

The ages above are examples, not library defaults. Deletion is permanent: the
payload, the result and the attempt history disappear, a deleted failure cannot
be retried, a later `get` reports `None`, and the freed idempotency key can
create a new task. The queue scope is not an authorization boundary; the caller
owns authorization and any application record that references a deleted task
must tolerate the missing task.

### The transaction API

Every mutating operation has a variant that runs inside the transaction of the
caller: `enqueue_in`, `claim_in`, `renew_in`, `report_progress_in`,
`complete_in`, `fail_in`, `request_cancel_in`, `request_pause_in`, `resume_in`,
`park_paused_in`, `release_in`, `retry_failed_in`, `delete_terminal_in`,
`prune_terminal_in`, `recover_expired_in` and `recover_expired_by_id_in`.

```rust
use sea_orm::TransactionTrait;

let mut transaction = db.begin().await?;
queue.enqueue_in(&mut transaction, request).await?;
write_business_row(&mut transaction).await?;
transaction.commit().await?;
```

A variant never begins, commits or rolls back a transaction, so its result is
valid before the commit of the caller but not durable until it. The transaction
must belong to the schema instance of the queue, otherwise the call is refused
before it writes. A refused operation must not be ignored: roll the transaction
back, otherwise the business side effects of an attempt that lost its lease are
committed.

A variant accepts a SeaORM transaction or a native SQLx transaction of the
caller:

```rust
use sea_orm::sqlx;

let mut transaction = pool.begin().await?;
queue.enqueue_in(&mut transaction, request).await?;
write_business_row(&mut transaction).await?;
transaction.commit().await?;
```

The library writes its statements on the connection of the caller. It never
begins a second transaction, never commits or rolls back the transaction of the
caller and never checks a connection out of a pool on that path, so a caller
that holds its only connection keeps its atomicity.

### Scoped reads and addressed recovery

`get_in`, `get_many_in` and `attempts_in` read the tasks and attempts of the
queue in the transaction of the caller without a row lock. `lock_task_in` locks
one task for update, and `has_live_attempts_in` locks every distinct task of a
batch for share and reports whether any of them holds a running attempt with an
unexpired lease. The batch reads accept at most `MAX_TASK_BATCH_SIZE` (512)
identifiers, read duplicate identifiers once and order their result by task id.
An absent or out-of-scope task is omitted by `get_many_in` and refused by
`has_live_attempts_in` rather than reported as without a live attempt.
`get_by_idempotency_key_in` reads one task by its exact idempotency key in the
transaction of the caller. The key follows the validation of an enqueue key; a
key that is absent or belongs to another namespace or queue is reported as
`None`.

`recover_expired_by_id` and `recover_expired_by_id_in` recover only the named
task. They seal its expired running attempt with the same rules as
`recover_expired`, never fall back to another task and never revoke a live
lease.

### The activity read

`active_tasks_in` reads the active tasks of one kind in the transaction of the
caller, `active_task_in` reads a single one. A task is active when its status
is queued, running or cancel requested, or when it holds a running attempt
whose lease expires later than the lease grace of the caller before the clock
of the transaction. The grace must be positive and at most one hour.

```rust
let mut transaction = db.begin().await?;
let active = queue
    .active_tasks_in(&mut transaction, &kind, Duration::from_secs(30))
    .await?;
transaction.commit().await?;
```

The read is scoped to the namespace and the queue of the queue handle and to
the given kind, and it keeps the payload of a task opaque. The result is
ordered by task id.

The read is complete or it fails. More than `MAX_TASK_BATCH_SIZE` (512) active
tasks of the kind are refused with `Error::Validation` instead of truncated, so
a caller never reads a partial result and never treats it as an absence of
activity. An empty result is a complete result. `active_task_in` reports a task
that is absent or belongs to another namespace, another queue or another kind
as `Error::NotFound`, and a task of the scope that is not active as `None`.

The read takes no row lock and it is a snapshot, not a guard. It does not stop
a claim, a recovery or a terminal writer, and it does not prevent a task of
the kind from appearing between the read and a later write. A caller that must
exclude that must lock the business identities of the task itself, in a
consistent order, and must not use this read as a substitute for those locks.

## Build

Persistence runs through SeaORM. Ordinary reads and writes use the private SeaORM
entities of the library; concurrency-critical statements, migrations and the
legacy migration ledger live in `sql/*.sql` and are executed as SeaORM
statements. No compile-time query metadata is needed, so the crate builds
offline:

```sh
cargo check --locked --offline --all-targets
```

The examples show the library on both sides:
`cargo run --example basic` and `cargo run --example transactional`.

The test suite is a set of isolated unit tests inside the source modules. It
needs no database and runs in milliseconds:

```sh
cargo test --locked --offline
```

The unit suite does not validate the behavior of PostgreSQL. The schema, the
SQL statements, the concurrency, the leases, the fencing and the migration
ledger are not covered by an automated test.

## License

Apache-2.0, see [LICENSE](LICENSE).
