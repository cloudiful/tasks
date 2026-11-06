-- cloudiful-tasks schema, protocol version 3.
--
-- A task carries an independent cooperative pause state and a pause
-- generation. An attempt parks as a sealed paused outcome and records the
-- generation it captured when it started. The additive columns preserve every
-- existing row.

ALTER TABLE cloudiful_tasks.tasks
    ADD COLUMN pause_state text NOT NULL DEFAULT 'unpaused',
    ADD COLUMN pause_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN paused_attempts integer NOT NULL DEFAULT 0,
    ADD CONSTRAINT tasks_pause_state_check
        CHECK (pause_state IN ('unpaused', 'pausing', 'paused')),
    ADD CONSTRAINT tasks_pause_generation_check
        CHECK (pause_generation >= 0),
    ADD CONSTRAINT tasks_paused_attempts_check
        CHECK (paused_attempts >= 0 AND paused_attempts <= attempt_count);

ALTER TABLE cloudiful_tasks.attempts
    ADD COLUMN pause_generation bigint NOT NULL DEFAULT 0,
    ADD COLUMN paused_at timestamptz;

ALTER TABLE cloudiful_tasks.attempts
    DROP CONSTRAINT attempts_status_check,
    ADD CONSTRAINT attempts_status_check
        CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted', 'paused')),
    ADD CONSTRAINT attempts_pause_generation_check
        CHECK (pause_generation >= 0),
    ADD CONSTRAINT attempts_paused_at_check
        CHECK ((status = 'paused') = (paused_at IS NOT NULL));

UPDATE cloudiful_tasks.schema_ownership
SET protocol_version = 3,
    updated_at = now()
WHERE singleton;
