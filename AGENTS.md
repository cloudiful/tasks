# Agent notes

`cloudiful-tasks` is a Rust 2024 library that stores tasks, attempts and leases
in one PostgreSQL schema. The crate is published as `cloudiful-tasks`, imported
as `cloudiful_tasks`, licensed Apache-2.0 and is not published to crates.io.

## Build and test

```sh
cargo fmt --all -- --check
cargo check --locked --offline --all-targets
cargo test --locked --offline --lib
cargo test --locked --offline --doc
SQLX_OFFLINE=true cargo check --locked --offline --all-targets
```

The live tests are ignored by default. They use the dedicated non-production
databases on the shared test server and run serially:

The live tests read two explicit environment values:
`CLOUDIFUL_TASKS_TEST_DATABASE_URL` for `cloudiful_tasks_test` and
`CLOUDIFUL_TASKS_TEST_SIBLING_DATABASE_URL` for `cloudiful_tasks_dev`.

```sh
cargo test --locked --offline --test migration -- --include-ignored --test-threads=1
cargo test --locked --offline --test postgres -- --include-ignored --test-threads=1
cargo test --locked --offline --test worker -- --include-ignored --test-threads=1
```

The test URL names `cloudiful_tasks_test`; the explicit sibling URL names
`cloudiful_tasks_dev`. Both must use the fixed `unraid.cloud1ful.com:5432`
endpoint. A live test fails when an environment value is missing or malformed,
when the effective `PgConnectOptions` carries a socket or connection option
override, or when the database does not report the configured role, a TCP
server address, server port `5432` and the exact dedicated database comment
(`cloudiful-tasks test fixture v1; non-production; dedicated` for the test
database, `cloudiful-tasks development fixture v1; non-production; dedicated`
for the sibling). The guard runs before any reset or destructive statement.

## Databases

| Database | Purpose |
| --- | --- |
| `cloudiful_tasks_dev` | Query metadata for `cargo sqlx prepare`, the compile time check of the query macros, and the sibling identity test |
| `cloudiful_tasks_test` | Live tests: migration, ownership and coexistence checks |

Only the development database is used to regenerate the query metadata:

```sh
DATABASE_URL="$CLOUDIFUL_TASKS_TEST_SIBLING_DATABASE_URL" \
  cargo sqlx prepare -- --all-targets
```

The development database also holds the test owned `app_domain` schema, so the
query macros of the test statements can be described. The fixture is created by
`sql/tests/transaction_app_domain_setup.sql`. Never reset a shared project
database and never derive one fixture URL from the other.

## Transaction variants

Every mutating queue operation has a variant that takes
`&mut sqlx::Transaction<'_, sqlx::Postgres>`: `enqueue_in`, `claim_in`,
`renew_in`, `report_progress_in`, `complete_in`, `fail_in`,
`request_cancel_in`, `retry_failed_in` and `recover_expired_in`.

- A variant verifies the schema instance of the queue before it writes and
  refuses a transaction of another database or of another schema instance.
- A variant never begins, commits or rolls back a transaction. The caller owns
  the commit and the rollback, so a refused operation must roll the transaction
  back.
- A variant shares its implementation with its self-committing counterpart
  through a private `*_on(&mut PgConnection, ...)` helper in `src/queue.rs`, so
  the two cannot diverge in result, error or bounds.
- The result of a variant is valid before the commit of the caller but not
  durable until it.

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
attempt, fencing token and a sealed `succeeded`, `failed` or `cancelled` status
with `sql/store_attempt_sealed.sql`. Only a verified seal skips the worker
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

## Tests

The live tests of `tests/postgres.rs` are ignored by default and run against
the fixture database with `--include-ignored --test-threads=1`. The borrowed
variants are covered with a domain sentinel: `tests/common/mod.rs` owns a
separate `app_domain` schema that the library never touches, so a business
write and the queue change commit or roll back together. A live test of a
borrowed variant must prove the atomicity of both directions and the refusal
of a wrong schema instance.

The statements of the sentinel live in `sql/tests/transaction_*.sql` and are
loaded with `query_file!` and `query_file_scalar!`, so they are checked like
the statements of the library. Only the DDL that creates the schema is loaded
through `raw_sql`.

## Rules

- Every statement of the library lives in `sql/*.sql` and is loaded with
  `query_file!` or `query_file_as!`. Inline SQL is only allowed for statements
  that cannot be expressed as a static file.
- The query metadata in `.sqlx` is committed, so the crate builds with
  `SQLX_OFFLINE=true` and without `DATABASE_URL`.
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
- The payload of a task is opaque data of the caller. The library never
  interprets it and never logs it.
- The worker runs only when `run` is called. Nothing starts in the background
  before that.
- Test reports of the implementation phases are written to `target/test-results/`.
