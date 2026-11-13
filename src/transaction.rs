//! The borrowed transaction of the caller.
//!
//! A caller that already owns a transaction can let the library write into
//! that transaction instead of into a transaction of its own. The handle is
//! closed: it is created from a SeaORM transaction or from a native SQLx
//! transaction and it exposes no task statement to the caller.
//!
//! The library never begins, commits or rolls back the borrowed transaction
//! and never checks a connection out of a pool on that path. The statements of
//! the library run on the connection of the caller, so the business write and
//! the task write are durable together or not at all.

use std::sync::Arc;

use sea_orm::sqlx;
use sea_orm::sqlx::postgres::{PgConnection, Postgres};
use sea_orm::{
    ConnectionTrait, DatabaseTransaction, DbBackend, DbErr, ExecResult, QueryResult, RuntimeErr,
    Statement, Values,
};
use sea_query_sqlx::SqlxValues;
use tokio::sync::Mutex;

/// The transaction of the caller that the library writes into.
///
/// The handle borrows the transaction of the caller. It is created by the
/// `*_in` operations of the queue from a `&mut DatabaseTransaction` or from a
/// `&mut sqlx::Transaction`, so a caller never constructs it directly.
///
/// The library never commits or rolls back the borrowed transaction. A caller
/// that uses one of the `*_in` operations must commit the transaction itself
/// and must roll it back when the operation is refused.
pub struct TaskTransaction<'a> {
    inner: TaskConnection<'a>,
}

/// The connection of the caller behind the handle.
///
/// The adapter carries the statements of the library over to the connection of
/// the caller. It stays inside the library, so a caller cannot reach the
/// statements of the library through the handle.
pub(crate) enum TaskConnection<'a> {
    SeaOrm(&'a DatabaseTransaction),
    Sqlx(Mutex<&'a mut PgConnection>),
}

impl<'a> TaskTransaction<'a> {
    pub(crate) fn connection(&self) -> &TaskConnection<'a> {
        &self.inner
    }
}

impl std::fmt::Debug for TaskTransaction<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.inner, f)
    }
}

impl std::fmt::Debug for TaskConnection<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskConnection::SeaOrm(_) => f.write_str("TaskConnection::SeaOrm"),
            TaskConnection::Sqlx(_) => f.write_str("TaskConnection::Sqlx"),
        }
    }
}

impl<'a> From<&'a mut DatabaseTransaction> for TaskTransaction<'a> {
    fn from(transaction: &'a mut DatabaseTransaction) -> Self {
        Self {
            inner: TaskConnection::SeaOrm(transaction),
        }
    }
}

impl<'a> From<&'a mut sqlx::Transaction<'_, Postgres>> for TaskTransaction<'a> {
    fn from(transaction: &'a mut sqlx::Transaction<'_, Postgres>) -> Self {
        Self {
            inner: TaskConnection::Sqlx(Mutex::new(&mut **transaction)),
        }
    }
}

#[async_trait::async_trait]
impl ConnectionTrait for TaskConnection<'_> {
    fn get_database_backend(&self) -> DbBackend {
        DbBackend::Postgres
    }

    async fn execute_raw(&self, stmt: Statement) -> Result<ExecResult, DbErr> {
        match self {
            TaskConnection::SeaOrm(transaction) => transaction.execute_raw(stmt).await,
            TaskConnection::Sqlx(connection) => {
                let mut connection = connection.lock().await;
                sqlx_query(&stmt)
                    .execute(&mut **connection)
                    .await
                    .map(ExecResult::from)
                    .map_err(exec_error)
            }
        }
    }

    async fn execute_unprepared(&self, sql: &str) -> Result<ExecResult, DbErr> {
        match self {
            TaskConnection::SeaOrm(transaction) => transaction.execute_unprepared(sql).await,
            TaskConnection::Sqlx(connection) => {
                let mut connection = connection.lock().await;
                sqlx::Executor::execute(&mut **connection, sqlx::AssertSqlSafe(sql.to_owned()))
                    .await
                    .map(ExecResult::from)
                    .map_err(exec_error)
            }
        }
    }

    async fn query_one_raw(&self, stmt: Statement) -> Result<Option<QueryResult>, DbErr> {
        match self {
            TaskConnection::SeaOrm(transaction) => transaction.query_one_raw(stmt).await,
            TaskConnection::Sqlx(connection) => {
                let mut connection = connection.lock().await;
                sqlx_query(&stmt)
                    .fetch_optional(&mut **connection)
                    .await
                    .map(|row| row.map(QueryResult::from))
                    .map_err(query_error)
            }
        }
    }

    async fn query_all_raw(&self, stmt: Statement) -> Result<Vec<QueryResult>, DbErr> {
        match self {
            TaskConnection::SeaOrm(transaction) => transaction.query_all_raw(stmt).await,
            TaskConnection::Sqlx(connection) => {
                let mut connection = connection.lock().await;
                sqlx_query(&stmt)
                    .fetch_all(&mut **connection)
                    .await
                    .map(|rows| rows.into_iter().map(QueryResult::from).collect())
                    .map_err(query_error)
            }
        }
    }
}

fn sqlx_query(stmt: &Statement) -> sqlx::query::Query<'_, Postgres, SqlxValues> {
    let values = stmt
        .values
        .as_ref()
        .map_or(Values(Vec::new()), |values| values.clone());
    sqlx::query_with(sqlx::AssertSqlSafe(stmt.sql.as_str()), SqlxValues(values))
}

fn exec_error(error: sqlx::Error) -> DbErr {
    DbErr::Exec(RuntimeErr::SqlxError(Arc::new(error)))
}

fn query_error(error: sqlx::Error) -> DbErr {
    DbErr::Query(RuntimeErr::SqlxError(Arc::new(error)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::sqlx::postgres::Postgres;

    fn from_seaorm(transaction: &mut DatabaseTransaction) -> TaskTransaction<'_> {
        transaction.into()
    }

    fn from_sqlx<'a, 'c>(
        transaction: &'a mut sqlx::Transaction<'c, Postgres>,
    ) -> TaskTransaction<'a> {
        transaction.into()
    }

    #[test]
    fn the_handle_is_created_from_a_seaorm_and_from_a_sqlx_transaction() {
        let _ = from_seaorm;
        let _ = from_sqlx;
    }
}
