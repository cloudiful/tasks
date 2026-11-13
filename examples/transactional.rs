//! The caller side of the library: a business write and the task of the
//! library share one transaction, and a completion fences the business write
//! of the attempt.
//!
//! The example needs a database with the schema of the library:
//!
//! ```sh
//! export DATABASE_URL=postgresql://dev@localhost:57664/cloudiful_tasks_dev
//! cargo run --example transactional
//! ```

use cloudiful_tasks::{EnqueueRequest, Queue, TaskKind, WorkerConfig, check_schema};
use sea_orm::sqlx;
use sea_orm::sqlx::postgres::PgPoolOptions;
use sea_orm::{ConnectionTrait, DatabaseConnection, DbBackend, Statement, TransactionTrait};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;

    // The caller owns one pool. The connection of the library reuses that
    // pool, so the borrowed transaction below runs on a connection of the same
    // pool and the connection budget of the caller stays unchanged.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;
    let db: DatabaseConnection = pool.clone().into();

    let schema = check_schema(&db).await?;
    println!("schema instance {}", schema.instance_uuid());

    // The table of the application, the library never writes to it.
    db.execute_unprepared(
        "CREATE TABLE IF NOT EXISTS public.example_orders (id uuid PRIMARY KEY, state text NOT NULL)",
    )
    .await?;

    let queue = Queue::open(db.clone(), "stock", "k8s", &WorkerConfig::default()).await?;

    // The business write and the task are committed together. When the
    // transaction rolls back, neither of them is durable.
    let order_id = uuid::Uuid::new_v4();
    let mut transaction = db.begin().await?;
    transaction
        .execute_raw(Statement::from_sql_and_values(
            DbBackend::Postgres,
            "INSERT INTO public.example_orders (id, state) VALUES ($1, 'new')",
            [order_id.into()],
        ))
        .await?;
    queue
        .enqueue_in(
            &mut transaction,
            EnqueueRequest::new(TaskKind::new("sync")?, json!({"order": order_id})),
        )
        .await?;
    transaction.commit().await?;

    // The same borrowed transaction works with a native SQLx transaction of the
    // caller. The library writes its statements on the connection of the
    // caller, so the business write and the task write stay atomic and the
    // library never checks a connection out of a pool.
    let sqlx_order_id = uuid::Uuid::new_v4();
    let mut sqlx_transaction = pool.begin().await?;
    sqlx::query("INSERT INTO public.example_orders (id, state) VALUES ($1, 'new')")
        .bind(sqlx_order_id)
        .execute(&mut *sqlx_transaction)
        .await?;
    queue
        .enqueue_in(
            &mut sqlx_transaction,
            EnqueueRequest::new(TaskKind::new("sync")?, json!({"order": sqlx_order_id})),
        )
        .await?;
    sqlx_transaction.commit().await?;

    // A refused or failed business write rolls the task back as well. The
    // caller owns the rollback, the library never rolls back for the caller.
    let rolled_back_order_id = uuid::Uuid::new_v4();
    let mut sqlx_transaction = pool.begin().await?;
    sqlx::query("INSERT INTO public.example_orders (id, state) VALUES ($1, 'new')")
        .bind(rolled_back_order_id)
        .execute(&mut *sqlx_transaction)
        .await?;
    queue
        .enqueue_in(
            &mut sqlx_transaction,
            EnqueueRequest::new(
                TaskKind::new("sync")?,
                json!({"order": rolled_back_order_id}),
            ),
        )
        .await?;
    sqlx_transaction.rollback().await?;

    // The worker of the library completes the attempt in its own transaction.
    // An application that writes its own state in the same transaction as the
    // completion uses `complete_in` and must roll the transaction back when
    // the completion is refused: a refused completion means that the attempt
    // lost its lease, its side effects must not be committed.
    println!("order {order_id} is queued");
    Ok(())
}
