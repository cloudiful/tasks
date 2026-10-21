WITH guard AS (
    SELECT id, status
    FROM cloudiful_tasks.tasks
    WHERE id = $3 AND namespace = $1 AND queue = $2
    FOR UPDATE
),
sealed AS (
    UPDATE cloudiful_tasks.attempts AS a
    SET status = CASE WHEN guard.status = 'cancel_requested' THEN 'cancelled' ELSE 'succeeded' END,
        finished_at = clock_timestamp(),
        lease_expires_at = NULL
    FROM guard
    WHERE a.id = $4
      AND a.task_id = guard.id
      AND a.fencing_token = $5
      AND a.status = 'running'
      AND a.lease_expires_at > clock_timestamp()
    RETURNING a.task_id
)
UPDATE cloudiful_tasks.tasks AS t
SET status = CASE WHEN t.status = 'cancel_requested' THEN 'cancelled' ELSE 'succeeded' END,
    result = CASE WHEN t.status = 'cancel_requested' THEN NULL ELSE $6::jsonb END,
    finished_at = clock_timestamp(),
    updated_at = clock_timestamp(),
    terminal_reason = CASE WHEN t.status = 'cancel_requested' THEN 'cancelled' ELSE t.terminal_reason END
FROM sealed
WHERE t.id = sealed.task_id
RETURNING t.*
