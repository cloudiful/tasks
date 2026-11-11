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
  cooperative per-task pause and resume, and recovery of expired leases,
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
sqlx = { version = "0.9", default-features = false, features = ["postgres", "runtime-tokio", "macros", "migrate", "json", "uuid", "time"] }
tokio-util = "0.7"
```

The public API takes a `sqlx::PgPool`, so the application depends on `sqlx`
directly. Timestamps are `time::OffsetDateTime` and must be representable in UTC
within the default `time` year range (-9999 through 9999); PostgreSQL values
outside that range are not supported. A persisted availability timestamp uses
UTC with exactly six fractional digits and a `Z` suffix, so the same request
keeps the same identity across builds.

## Usage

### The schema

The schema is created explicitly. Opening a queue or starting a worker never
creates or alters schema.

```rust
use cloudiful_tasks::{check_schema, migrate};
use sqlx::postgres::PgPoolOptions;

let pool = PgPoolOptions::new().max_connections(5).connect("postgresql://user@localhost/tasks").await?;
migrate(&pool).await?;
check_schema(&pool).await?;
```

`migrate` is idempotent and serialized per database: it creates a missing schema
and upgrades a supported older version. `check_schema` only reads and reports
`SchemaMissing` or `OutdatedSchemaVersion` instead. Both refuse foreign schemas
(`ForeignSchema`) and newer protocols (`UnsupportedSchemaVersion`); a schema
that the library does not own is never adopted or overwritten.

### The queue

A queue addresses the tasks of one namespace and one queue name.

```rust
use cloudiful_tasks::{EnqueueRequest, Queue, TaskKind, WorkerConfig};
use serde_json::json;

let queue = Queue::open(pool.clone(), "stock", "k8s", &WorkerConfig::default()).await?;
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
let queue = Queue::open(pool.clone(), "stock", "k8s", &config).await?;
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

### The transaction API

Every mutating operation has a variant that runs inside the transaction of the
caller: `enqueue_in`, `claim_in`, `renew_in`, `report_progress_in`,
`complete_in`, `fail_in`, `request_cancel_in`, `request_pause_in`, `resume_in`,
`park_paused_in`, `retry_failed_in` and `recover_expired_in`.

```rust
let mut transaction = pool.begin().await?;
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

## Build

All SQL of the library lives in `sql/*.sql` and is loaded through `query_file!`
and `query_file_as!`. The query metadata in `.sqlx` is committed, so the crate
builds without a database:

```sh
SQLX_OFFLINE=true cargo check --locked --offline --all-targets
```

Regenerate the metadata only against the dedicated development database of the
shared non-production test server:

```sh
DATABASE_URL="$TASKS_TEST_SIBLING_DATABASE_URL" cargo sqlx prepare -- --all-targets
```

The live suites read their fixture URLs from `TASKS_TEST_DATABASE_URL` and
`TASKS_TEST_SIBLING_DATABASE_URL`. The examples show the library on both sides:
`cargo run --example basic` and `cargo run --example transactional`.

## License

Apache-2.0, see [LICENSE](LICENSE).
