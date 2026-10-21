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
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let schema = check_schema(&pool).await?;
    println!("schema instance {}", schema.instance_uuid());

    // The table of the application, the library never writes to it.
    sqlx::raw_sql(
        "CREATE TABLE IF NOT EXISTS public.example_orders (id uuid PRIMARY KEY, state text NOT NULL)",
    )
    .execute(&pool)
    .await?;

    let queue = Queue::open(pool.clone(), "stock", "k8s", &WorkerConfig::default()).await?;

    // The business write and the task are committed together. When the
    // transaction rolls back, neither of them is durable.
    let order_id = uuid::Uuid::new_v4();
    let mut transaction = pool.begin().await?;
    sqlx::query("INSERT INTO public.example_orders (id, state) VALUES ($1, 'new')")
        .bind(order_id)
        .execute(&mut *transaction)
        .await?;
    queue
        .enqueue_in(
            &mut transaction,
            EnqueueRequest::new(TaskKind::new("sync")?, json!({"order": order_id})),
        )
        .await?;
    transaction.commit().await?;

    // The worker of the library completes the attempt in its own transaction.
    // An application that writes its own state in the same transaction as the
    // completion uses `complete_in` and must roll the transaction back when
    // the completion is refused: a refused completion means that the attempt
    // lost its lease, its side effects must not be committed.
    println!("order {order_id} is queued");
    Ok(())
}
