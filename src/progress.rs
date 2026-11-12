//! Progress reports of a running attempt.
//!
//! A progress report belongs to the attempt that wrote it. A new attempt
//! starts without progress, a sealed attempt keeps its last report.

use sea_orm::ConnectionTrait;
use serde_json::Value;

use crate::error::Result;
use crate::model::{Attempt, Namespace, ProgressUpdate, QueueName};
use crate::sql;
use crate::store;

const REPORT_PROGRESS: &str = include_str!("../sql/store_report_progress.sql");

/// Writes the progress report of a running attempt.
pub(crate) async fn report_progress<C: ConnectionTrait>(
    connection: &C,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    progress: &ProgressUpdate,
) -> Result<()> {
    let payload = progress_payload(progress);
    let row = connection
        .query_one_raw(sql::statement(
            REPORT_PROGRESS,
            vec![
                namespace.as_str().into(),
                queue.as_str().into(),
                attempt.task_id().as_uuid().into(),
                attempt.id().as_uuid().into(),
                attempt.fencing_token().as_uuid().into(),
                payload.into(),
            ],
        ))
        .await?;
    if row.is_some() {
        return Ok(());
    }
    Err(store::lease_failure(connection, namespace, queue, attempt.task_id()).await)
}

/// The stored representation of a progress report.
fn progress_payload(progress: &ProgressUpdate) -> Value {
    serde_json::to_value(progress).unwrap_or(Value::Null)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_payload_of_a_report_holds_its_parts() {
        let report = ProgressUpdate::new("import")
            .with_completed(3)
            .with_total(10)
            .with_unit("pages");
        let payload = progress_payload(&report);

        assert_eq!(payload["stage"], "import");
        assert_eq!(payload["completed"], 3);
        assert_eq!(payload["total"], 10);
        assert_eq!(payload["unit"], "pages");
    }
}
