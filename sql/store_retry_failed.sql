-- Grants the requested additional attempts to a failed task.
--
-- The new budget is the number of the attempts that ran plus the requested
-- additional attempts, the unused budget of the task is not granted again.
-- The limit is checked, the sum of both numbers cannot overflow the column.
UPDATE cloudiful_tasks.tasks
SET status = 'queued',
    max_attempts = LEAST(attempt_count::bigint + $4::bigint, $5::bigint)::integer,
    available_at = clock_timestamp(),
    finished_at = NULL,
    terminal_reason = NULL,
    result = NULL,
    updated_at = clock_timestamp()
WHERE id = $3 AND namespace = $1 AND queue = $2 AND status = 'failed'
RETURNING *
