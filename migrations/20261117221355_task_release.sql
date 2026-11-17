-- cloudiful-tasks schema, protocol version 4.
--
-- A task carries the number of the attempts that their owner released. An
-- attempt that was released keeps its place in the history of the task and
-- does not consume the failure budget of the task. The additive column
-- preserves every existing row.

ALTER TABLE cloudiful_tasks.tasks
    ADD COLUMN released_attempts integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT tasks_released_attempts_check
        CHECK (released_attempts >= 0 AND released_attempts <= attempt_count),
    ADD CONSTRAINT tasks_attempt_budget_check
        CHECK (paused_attempts + released_attempts <= attempt_count);

ALTER TABLE cloudiful_tasks.attempts
    DROP CONSTRAINT attempts_status_check,
    ADD CONSTRAINT attempts_status_check
        CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted', 'paused', 'released'));

UPDATE cloudiful_tasks.schema_ownership
SET protocol_version = 4,
    updated_at = now()
WHERE singleton;
