# cloudiful-tasks

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

[English](README.md) | [简体中文](README.zh-CN.md)

Durable task execution on PostgreSQL.

`cloudiful-tasks` keeps tasks, their attempts and their leases in one
PostgreSQL schema that the library owns. The library never runs work on its
own: a worker runtime is started explicitly by the application.

Execution is **at least once**. A worker that loses its lease can still finish
its work, so handlers must be idempotent and side effects must be fenced with
the fencing token of the attempt.

## Status

The `v0.1` release implements:

- the data model: namespaces, queues, task kinds, task and attempt states,
  retry policies, progress reports and fencing tokens,
- the worker configuration contract with its validation rules,
- bounded, overflow safe exponential backoff,
- an explicit schema bootstrap with an ownership marker, a versioned schema
  protocol and migration checksum verification,
- the durable queue store: enqueue with idempotency, claim with a lease,
  renewal, progress, completion, failure, cancellation, manual retry and
  recovery of expired leases,
- the worker runtime: bounded concurrency, lease heartbeat, cooperative
  cancellation, recovery of expired leases, wake up notifications and a
  graceful shutdown.

## Install

The crate is not published to crates.io. Use the Git dependency. The repository
has no release yet, so the dependency pins the commit it was verified with:

```toml
[dependencies]
cloudiful-tasks = { git = "https://github.com/cloudiful/tasks", rev = "<COMMIT_SHA>" }
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio", "macros", "migrate", "json", "uuid", "chrono"] }
tokio-util = "0.7"
```

Replace `<COMMIT_SHA>` with the commit you verified. The application depends on
`sqlx` directly, because the public API takes a `sqlx::PgPool`.

## Usage

### The schema

The schema is created explicitly. Opening a queue or starting a worker never
creates or alters schema.

```rust
use cloudiful_tasks::{check_schema, migrate};
use sqlx::postgres::PgPoolOptions;

let pool = PgPoolOptions::new()
    .max_connections(5)
    .connect("postgresql://user@localhost/tasks")
    .await?;

migrate(&pool).await?;
let schema = check_schema(&pool).await?;
println!("schema instance {} at version {}", schema.instance_uuid(), schema.protocol_version());
```

`migrate` is idempotent and serialized per database, so concurrent callers
apply the migrations exactly once. `check_schema` only reads.

Both calls refuse a database that is not ready:

| Situation | Result |
| --- | --- |
| The schema does not exist | `check_schema` reports `SchemaMissing`, `migrate` creates it |
| The schema exists without the ownership marker of the library | `ForeignSchema` |
| The schema was written by a newer library version | `UnsupportedSchemaVersion` |
| An applied migration no longer matches the migration source | `ChecksumMismatch` |

A schema that the library does not own is never adopted and never overwritten.

### The queue

A queue addresses the tasks of one namespace and one queue name. Every
operation of the queue is scoped that way.

```rust
use cloudiful_tasks::{EnqueueRequest, ProgressUpdate, Queue, TaskKind, WorkerConfig};
use serde_json::json;

let queue = Queue::open(pool.clone(), "stock", "k8s", &WorkerConfig::default()).await?;

let outcome = queue
    .enqueue(EnqueueRequest::new(TaskKind::new("sync")?, json!({"cluster": "a"})))
    .await?;

if let Some(claim) = queue.claim("worker-1").await? {
    queue.renew(claim.attempt()).await?;
    queue
        .report_progress(claim.attempt(), &ProgressUpdate::new("pull").with_total(10))
        .await?;
    queue.complete(claim.attempt(), Some(json!({"ok": true}))).await?;
}
```

The semantics of the store:

| Topic | Behaviour |
| --- | --- |
| Idempotency | The same key with the same request returns the existing task, also when it already finished. The same key with a different request is an `IdempotencyConflict`. A request without a key always creates a task. |
| Claim | Only a ready task is claimed, highest priority first, then the oldest ready one. The claim holds the lease of the task and carries a fresh fencing token. |
| Lease | The lease expires on the clock of the database. A renewal, a progress report, a completion and a failure need the attempt, its token and an unexpired lease, otherwise the call reports `LeaseLost` and changes nothing. The lease is checked against the clock of the database after the locks of the operation are taken, so a call that waits for a lock cannot use an expired lease. |
| Recovery | An expired attempt is sealed as interrupted and its task is scheduled again while the budget of the task allows it, otherwise the task fails. The recovery is idempotent. |
| Cancellation | A queued task is cancelled at once, a running task is marked for cancellation. A cancellation that was requested before the completion wins: the task ends as cancelled and its result is discarded. External side effects cannot be undone. |
| Retry | A failure is permanent unless the caller marks it retryable, and a task is only retried again when the caller grants additional attempts with `retry_failed`. The new budget is the number of the attempts that ran plus the granted attempts, an unused budget is not granted again and the budget can shrink. The attempt history and the request of the task stay untouched. |

### The worker

A worker claims the tasks of its queue and runs the handlers of their kinds.
Nothing starts before `run` is called.

```rust
use async_trait::async_trait;
use cloudiful_tasks::{
    CancellationToken, HandlerError, Outcome, ProgressUpdate, Queue, TaskContext, TaskHandler,
    TaskKind, Worker, WorkerConfig,
};
use serde_json::json;

struct Sync;

#[async_trait]
impl TaskHandler for Sync {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        context
            .progress(&ProgressUpdate::new("sync").with_total(2))
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;
        Ok(Outcome::completed(Some(json!({"synced": true}))))
    }
}

let config = WorkerConfig::default();
let queue = Queue::open(pool.clone(), "stock", "k8s", &config).await?;
let worker = Worker::builder(queue, config)
    .handler(TaskKind::new("sync")?, Sync)?
    .build()?;

let shutdown = CancellationToken::new();
worker.run(shutdown).await?;
```

The semantics of the worker:

| Topic | Behaviour |
| --- | --- |
| Start | The worker runs only when `run` is called. Opening a queue or building a worker starts nothing. |
| Concurrency | The number of running handlers is bounded by the configuration. A free slot takes the next ready task at once. |
| Heartbeat | The worker renews the lease of its running attempts. The renewal is bounded by the lease that is left. A lost lease, a renewal that did not finish in time or repeated database errors stop the handler and block its write, also while the lease of the database is still live. |
| Cancellation | The handler observes the token of its attempt. A cancelled task stops its handler. |
| Recovery | The worker seals the attempts whose lease expired and schedules their tasks again. |
| Wake up | The notification of the queue is a hint. The worker also polls, so a lost notification only delays the next task. |
| A panic | The task fails permanently, the worker keeps running. The payload of the task is never printed. |
| Shutdown | The worker stops claiming, asks its handlers to stop, waits for the grace period and aborts the rest. The aborted handlers are dropped, also when they ignore the cancellation token of their attempt. Their leases stay recoverable, no task is finished with a result the worker did not produce. |

### The transaction API

`enqueue_in`, `complete_in` and `fail_in` run inside the transaction of the
caller, so a business write and the task share one commit:

```rust
let mut transaction = pool.begin().await?;
queue.enqueue_in(&mut transaction, request).await?;
write_business_row(&mut transaction).await?;
transaction.commit().await?;
```

The transaction must belong to the database of the queue, otherwise the call
is refused. A refused completion must not be ignored: roll the transaction
back, otherwise the business side effects of an attempt that lost its lease
are committed.

Two examples show both sides of the library:

```sh
cargo run --example basic
cargo run --example transactional
```

## Build

All SQL of the library lives in `sql/*.sql` and is loaded through
`query_file!` and `query_file_as!`. The query metadata in `.sqlx` is
committed, so the crate builds without a database:

```sh
SQLX_OFFLINE=true cargo check --locked --offline --all-targets
```

Regenerate the metadata only against the development database of the checkout:

```sh
DATABASE_URL=postgresql://dev@localhost:5432/cloudiful_tasks_dev cargo sqlx prepare -- --all-targets
```

## License

Apache-2.0, see [LICENSE](LICENSE).
