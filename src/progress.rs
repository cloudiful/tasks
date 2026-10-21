//! Progress reports of a running attempt.
//!
//! A progress report belongs to the attempt that wrote it. A new attempt
//! starts without progress, a sealed attempt keeps its last report.

use serde_json::Value;
use sqlx::PgConnection;

use crate::error::Result;
use crate::model::{Attempt, Namespace, ProgressUpdate, QueueName};
use crate::store;

/// Writes the progress report of a running attempt.
pub(crate) async fn report_progress(
    connection: &mut PgConnection,
    namespace: &Namespace,
    queue: &QueueName,
    attempt: &Attempt,
    progress: &ProgressUpdate,
) -> Result<()> {
    let payload = progress_payload(progress);
    let row = sqlx::query_file!(
        "sql/store_report_progress.sql",
        namespace.as_str(),
        queue.as_str(),
        attempt.task_id().as_uuid(),
        attempt.id().as_uuid(),
        attempt.fencing_token().as_uuid(),
        payload,
    )
    .fetch_optional(&mut *connection)
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
