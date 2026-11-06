-- Grants the requested additional attempts to a failed task.
--
-- The new budget is the effective number of attempts that ran plus the
-- requested additional attempts, the unused budget of the task is not granted
-- again. The effective number of attempts excludes the attempts that parked at
-- a cooperative pause checkpoint, so a parked attempt never consumes the
-- failure budget. The limit is checked, the sum cannot overflow the column.
UPDATE cloudiful_tasks.tasks
SET status = 'queued',
    max_attempts = LEAST(
        (attempt_count - paused_attempts)::bigint + $4::bigint,
        $5::bigint
    )::integer,
    pause_state = 'unpaused',
    available_at = clock_timestamp(),
    finished_at = NULL,
    terminal_reason = NULL,
    result = NULL,
    updated_at = clock_timestamp()
WHERE id = $3 AND namespace = $1 AND queue = $2 AND status = 'failed'
RETURNING *
