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

The live tests are ignored by default. They need the fixture database and run
serially:

```sh
CLOUDIFUL_TASKS_TEST_DATABASE_URL='postgresql://dev@localhost:57664/cloudiful_tasks_test?host=<repo>/target/pg' \
  cargo test --locked --offline --test migration -- --include-ignored --test-threads=1
CLOUDIFUL_TASKS_TEST_DATABASE_URL='postgresql://dev@localhost:57664/cloudiful_tasks_test?host=<repo>/target/pg' \
  cargo test --locked --offline --test postgres -- --include-ignored --test-threads=1
CLOUDIFUL_TASKS_TEST_DATABASE_URL='postgresql://dev@localhost:57664/cloudiful_tasks_test?host=<repo>/target/pg' \
  cargo test --locked --offline --test worker -- --include-ignored --test-threads=1
```

`<repo>` is the root of this checkout; the socket directory is `<repo>/target/pg`.
A live test fails when the variable is missing or when the database does not
report the expected identity (`cloudiful_tasks_test`, role `dev`, port `57664`,
no network listener, socket of this checkout). Never point the variable at a
production or shared database.

## Databases

| Database | Purpose |
| --- | --- |
| `cloudiful_tasks_dev` | Query metadata for `cargo sqlx prepare`, and the compile time check of the query macros |
| `cloudiful_tasks_test` | Live tests: migration, ownership and coexistence checks |

Only the development database is used to regenerate the query metadata:

```sh
DATABASE_URL='postgresql://dev@localhost:57664/cloudiful_tasks_dev?host=<repo>/target/pg' \
  cargo sqlx prepare -- --all-targets
```

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
