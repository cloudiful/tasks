UPDATE cloudiful_tasks.tasks
SET status = CASE
        WHEN status = 'queued' THEN 'cancelled'
        WHEN status = 'running' THEN 'cancel_requested'
        ELSE status
    END,
    cancel_requested_at = CASE
        WHEN status = 'queued' THEN clock_timestamp()
        ELSE COALESCE(cancel_requested_at, clock_timestamp())
    END,
    finished_at = CASE WHEN status = 'queued' THEN clock_timestamp() ELSE finished_at END,
    terminal_reason = CASE WHEN status = 'queued' THEN 'cancelled' ELSE terminal_reason END,
    updated_at = clock_timestamp()
WHERE id = $3 AND namespace = $1 AND queue = $2
  AND status IN ('queued', 'running', 'cancel_requested')
RETURNING *
