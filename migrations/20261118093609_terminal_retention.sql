-- cloudiful-tasks schema, protocol version 5.
--
-- Terminal cleanup selects old succeeded, failed and cancelled tasks by their
-- finished time and deletes a bounded batch. The partial index covers only the
-- terminal rows and preserves every task, attempt, payload and lease.

CREATE INDEX tasks_terminal_finished_idx
    ON cloudiful_tasks.tasks (namespace, queue, finished_at, id)
    WHERE status IN ('succeeded', 'failed', 'cancelled') AND finished_at IS NOT NULL;

UPDATE cloudiful_tasks.schema_ownership
SET protocol_version = 5,
    updated_at = now()
WHERE singleton;
