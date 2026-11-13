# Agent notes

`cloudiful-tasks` is a Rust 2024 library that stores tasks, attempts and leases
in one PostgreSQL schema. The crate is published as `cloudiful-tasks`, imported
as `cloudiful_tasks`, licensed Apache-2.0 and is not published to crates.io.

## Build and test

```sh
cargo fmt --all -- --check
cargo check --locked --offline --all-targets
cargo test --locked --offline
cargo test --locked --offline --doc
```

The test suite is a set of isolated unit tests inside the source modules. It
needs no database, no credentials and no fixture provisioning, and it runs in
milliseconds. The unit suite does not validate the behavior of PostgreSQL: the
schema, the SQL statements, the concurrency, the leases, the fencing and the
migration ledger are not covered by an automated test.

## Databases

| Database | Purpose |
| --- | --- |
| `cloudiful_tasks_dev` | Manual inspection and the examples |

The library never creates a database. The application supplies the connection
and owns the database; the library owns the schema `cloudiful_tasks` inside it.
Never reset a shared project database.

## Transaction variants

Every mutating queue operation has a variant that takes the transaction of the
caller: `enqueue_in`, `claim_in`, `renew_in`, `report_progress_in`,
`complete_in`, `fail_in`, `request_cancel_in`, `request_pause_in`, `resume_in`,
`park_paused_in`, `retry_failed_in`, `recover_expired_in` and
`recover_expired_by_id_in`.

The variant takes a `&mut sea_orm::DatabaseTransaction` or a
`&mut sea_orm::sqlx::Transaction<'_, Postgres>`.

- A variant verifies the schema instance of the queue before it writes and
  refuses a transaction of another database or of another schema instance.
- A variant never begins, commits or rolls back a transaction. The caller owns
  the commit and the rollback, so a refused operation must roll the transaction
  back.
- A variant shares its implementation with its self-committing counterpart
  through a private `*_on<C: ConnectionTrait>(&self, &C, ...)` helper in
  `src/queue.rs`, so the two cannot diverge in result, error or bounds.
- The result of a variant is valid before the commit of the caller but not
  durable until it.
- A borrowed transaction runs on the connection of the caller. The library
  checks no connection out of a pool on that path, so a caller that holds its
  only connection keeps its atomicity. `TaskTransaction` is the closed handle
  of that path: it is created from a SeaORM transaction or from a native SQLx
  transaction and exposes no task statement to the caller.

## Scoped reads and batch locks

`get_in`, `get_many_in` and `attempts_in` read in the transaction of the caller
without a row lock. `lock_task_in` locks one task row for update and reports
`Error::NotFound` for an absent or out-of-scope task. `has_live_attempts_in`
locks every distinct task of its batch for share in identifier order, requires
every candidate to be present and then reports whether a running attempt has an
unexpired lease; an unknown task is refused, never reported as `false`.

`MAX_TASK_BATCH_SIZE` (512) bounds the identifiers of one batch before any
database work. Duplicate identifiers are accepted and read or locked once, and
results are ordered by task id. The shared and update locks last until the
caller ends its transaction, so they block claim, recovery and terminal writers
of those tasks.

`recover_expired_by_id` and `recover_expired_by_id_in` reuse
`sql/store_recover_expired.sql` with one optional target identifier. The
ordinary recovery binds `None`; the addressed form binds one target with a limit
of one and never falls back to another task.

## Cooperative pause

`TaskPauseState { Unpaused, Pausing, Paused }` is an independent task field; it
is not a `TaskStatus` variant. A queued task can be paused and stays nonterminal
and unclaimable. A running task enters `Pausing` until its attempt parks.

- `request_pause` locks the queue advisory lock and updates the task only. A
  queued task is paused at once; a running task keeps its lease and heartbeat.
  A repeated request keeps the generation, a terminal task is refused.
- `park_paused` locks the task and then the attempt, like complete and fail.
  It requires the exact running attempt, fence and an unexpired lease, seals
  the attempt `paused` with `paused_at`, and queues the task. A park that races
  a resume leaves the task unpaused and claimable; a cancellation that was
  requested before the park wins.
- `resume` clears `pausing`/`paused` without revoking the lease and without
  changing `available_at` or the attempt history. It does not advance the
  generation. A terminal task is refused.
- An attempt captures the task pause generation at claim. `TaskContext::
  pause_requested` is true only for its own task id while the state is pausing
  or paused and the task generation exceeds the captured one, so a resume
  clears the signal and one task's pause never stops another. The worker polls
  the state with the existing cancellation poll interval.
- `attempt_count` stays the monotonic physical total. `paused_attempts` counts
  only parked checkpoints, so every claim, failure, recovery and retry budget
  uses `attempt_count - paused_attempts`. An expired attempt of a pausing or
  paused task is sealed `paused` (unless a cancellation was requested) and
  never consumes the failure budget.

## Queue policies and external terminals

`QueuePolicy { max_concurrent_attempts: Option<u32> }` is stored in
`cloudiful_tasks.queue_policies` and read on every `Queue::policy` call. A
missing row is unlimited. Every claim path takes the queue scoped transaction
advisory lock and then requires `READ COMMITTED` before it reads the policy,
counts the running attempts of the queue and claims in separate statements; a
non-default isolation is refused even while the queue is unlimited, so an old
snapshot cannot bypass a limit stored after the transaction began. Recovery
keeps an expired attempt counted until it seals it.

`Outcome::ExternallyCompleted` means the handler already committed the terminal
of the captured attempt. The worker verifies the namespace, queue, task,
attempt, fencing token and a sealed `succeeded`, `failed`, `cancelled` or
`paused` status with `sql/store_attempt_sealed.sql`. Only a verified seal skips the worker
terminal; an unsealed signal uses the normal failure write and a lost lease
never touches a newer attempt.

## Addressed claims

`claim_by_id` and `claim_by_id_in` claim exactly one task of the queue, chosen
by its identifier.

- An addressed claim never falls back to another task. A target that is
  absent, of another namespace or queue, terminal, not ready, already running
  or locked by another transaction reports `None` and changes nothing.
- An addressed claim shares the statement and the lease, attempt and fencing
  mechanics of the ordinary claim: `sql/store_claim.sql` takes the target as
  an optional bind, `src/store.rs::claim_task` takes an optional `TaskId` and
  the private `claim_on` helper of `src/queue.rs` forwards `None` or `Some`.
- An addressed claim bypasses the order of the queue, never the rules of the
  queue. The decision which task is eligible stays with the caller, together
  with the locks that guard that decision.

## Activity read

`active_tasks_in` and `active_task_in` read the active tasks of one kind in the
transaction of the caller.

- A task is active when its status is `queued`, `running` or
  `cancel_requested`, or when it holds a running attempt whose
  `lease_expires_at` is later than `now() - lease_grace`. The lease is compared
  against the transaction clock, exactly like the predicate of
  `has_live_attempts_in`. The grace must be positive and at most
  `MAX_ACTIVITY_LEASE_GRACE` (one hour).
- The read is scoped to the namespace and the queue of the queue handle and to
  the given kind. The payload stays opaque and the result is ordered by task
  id. `active_task_in` reports an absent or out-of-scope task as
  `Error::NotFound` and a task of the scope that is not active as `None`.
- The read is complete or it fails. More than `MAX_TASK_BATCH_SIZE` (512)
  matching tasks are refused with `Error::Validation` instead of truncated, so
  a caller never treats a partial result as an absence of activity. An empty
  result is a complete result.
- The read takes no row lock. It is a snapshot and not a guard: it blocks no
  claim, recovery or terminal writer and it does not prevent a task of the
  kind from appearing between the read and a later write. A caller that needs
  that exclusion must lock the business identities of the task itself, in a
  consistent order, and must not use this read as a substitute.
- The statements live in `sql/store_active_tasks_of_kind.sql` and
  `sql/store_active_task_of_kind.sql` and run on the connection of the caller
  through the private adapter of `src/transaction.rs`.

## Tests

The test suite consists of isolated unit tests inside the source modules:
`src/migrate.rs`, `src/idempotency.rs`, `src/model.rs`, `src/config.rs`,
`src/backoff.rs`, `src/queue.rs`, `src/transaction.rs`, `src/progress.rs` and
`src/worker.rs`. They cover the bounds and the validation of the public values,
the serde contracts, the payload redaction, the canonicalization of the
idempotency key, the saturation of the backoff, the creation of the borrowed
transaction handle, the fencing registry of the worker and the integrity
decisions of the migration ledger. They need no database.

The unit suite does not validate PostgreSQL: the schema, the SQL statements,
the isolation, the leases, the pause and the recovery budgets, the worker
lifecycle and the migration ledger against a real database are not covered by
an automated test.

## Rules

- Ordinary persistence uses the private SeaORM entities in `src/entity/`.
  Concurrency-critical statements, guarded transitions, migrations and
  introspection live in `sql/*.sql` and are executed as SeaORM statements; they
  are only inline when they cannot be expressed as a static file.
- The borrowed transaction path runs the same statement files and the same
  store helpers on the connection of the caller, through the private adapter in
  `src/transaction.rs`. It never begins, commits or rolls back a transaction
  and never checks a connection out of a pool.
- `src/entity/schema.rs` holds the native SeaQuery declarations of the current
  schema. They are a public description of the schema, they never write the
  application schema and no test compares them with a migrated schema.
- The crate needs no compile-time query metadata and builds offline without a
  database.
- Migrations live in `migrations/` and use the actual system clock in the file
  name, `YYYYMMDDHHMMSS_lower_snake_case.sql`. An applied migration is never
  edited or renamed; a change is a new migration.
- The library owns the schema `cloudiful_tasks` and the migration ledger
  `cloudiful_tasks._sqlx_migrations`. Nothing is ever written to the public
  schema or to `public._sqlx_migrations`.
- Only `migrate` creates or alters the schema. Opening a queue or starting a
  worker never touches the schema.
- A schema without the ownership marker of the library is refused, never
  adopted.
- A ledger row with `success = false` is dirty and is refused before any
  pending migration; an unsuccessful migration is never retried.
- The payload of a task is opaque data of the caller. The library never
  interprets it and never logs it.
- The worker runs only when `run` is called. Nothing starts in the background
  before that.
- Test reports of the implementation phases are written to `target/test-results/`.
