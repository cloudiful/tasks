//! A worker that serves one kind of task.
//!
//! The example needs a database with the schema of the library:
//!
//! ```sh
//! export DATABASE_URL=postgresql://dev@localhost:57664/cloudiful_tasks_dev
//! cargo run --example basic
//! ```
//!
//! The worker claims the tasks of the queue `stock/k8s` and runs the handler
//! of the kind `sync`. The side effects of a handler must be idempotent: an
//! attempt can run again after it lost its lease.

use async_trait::async_trait;
use cloudiful_tasks::{
    CancellationToken, HandlerError, Outcome, ProgressUpdate, Queue, TaskContext, TaskHandler,
    TaskKind, Worker, WorkerConfig,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;

/// The handler of the kind `sync`.
struct Sync;

#[async_trait]
impl TaskHandler for Sync {
    async fn handle(&self, context: TaskContext) -> Result<Outcome, HandlerError> {
        println!(
            "handling task {} of kind {} in attempt {}",
            context.task_id(),
            context.kind(),
            context.attempt_number()
        );

        let report = ProgressUpdate::new("sync").with_completed(1).with_total(2);
        context
            .progress(&report)
            .await
            .map_err(|error| HandlerError::new(error.to_string()))?;

        // The work of the handler belongs to the application, it must be
        // idempotent and it must be fenced with the token of the attempt.
        Ok(Outcome::completed(Some(json!({"synced": true}))))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;

    let config = WorkerConfig::default();
    let queue = Queue::open(pool.clone(), "stock", "k8s", &config).await?;
    let worker = Worker::builder(queue, config)
        .handler(TaskKind::new("sync")?, Sync)?
        .build()?;

    let shutdown = CancellationToken::new();
    let signal = shutdown.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            signal.cancel();
        }
    });

    worker.run(shutdown).await?;
    Ok(())
}
