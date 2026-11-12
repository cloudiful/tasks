//! Raw SQL statements for the operations that need PostgreSQL semantics.

#![allow(missing_docs)]

use sea_orm::{DbBackend, Statement, Value};

pub(crate) fn statement(sql: &'static str, values: Vec<Value>) -> Statement {
    Statement::from_sql_and_values(DbBackend::Postgres, sql, values)
}
