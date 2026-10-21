WITH guard AS (
    SELECT id, status
    FROM cloudiful_tasks.tasks
    WHERE id = $3 AND namespace = $1 AND queue = $2
    FOR UPDATE
),
sealed AS (
    UPDATE cloudiful_tasks.attempts AS a
    SET status = CASE WHEN guard.status = 'cancel_requested' THEN 'cancelled' ELSE 'failed' END,
        finished_at = clock_timestamp(),
        lease_expires_at = NULL,
        error = $6::jsonb
    FROM guard
    WHERE a.id = $4
      AND a.task_id = guard.id
      AND a.fencing_token = $5
      AND a.status = 'running'
      AND a.lease_expires_at > clock_timestamp()
    RETURNING a.task_id
)
UPDATE cloudiful_tasks.tasks AS t
SET status = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        WHEN $7::boolean AND t.attempt_count < t.max_attempts THEN 'queued'
        ELSE 'failed'
    END,
    available_at = CASE
        WHEN t.status = 'cancel_requested' THEN t.available_at
        WHEN $7::boolean AND t.attempt_count < t.max_attempts
            THEN clock_timestamp() + ($8::bigint * interval '1 microsecond')
        ELSE t.available_at
    END,
    result = NULL,
    finished_at = CASE
        WHEN t.status = 'cancel_requested' THEN clock_timestamp()
        WHEN $7::boolean AND t.attempt_count < t.max_attempts THEN NULL
        ELSE clock_timestamp()
    END,
    terminal_reason = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        WHEN $7::boolean AND t.attempt_count < t.max_attempts THEN NULL
        ELSE 'failed'
    END,
    updated_at = clock_timestamp()
FROM sealed
WHERE t.id = sealed.task_id
RETURNING t.*
