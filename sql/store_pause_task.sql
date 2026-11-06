-- Requests the cooperative pause of one task.
--
-- A queued task is paused at once without an attempt, a running task enters
-- the pausing state until its attempt parks. A task that is already paused or
-- pausing keeps its generation, so a repeated request is idempotent. The
-- statement runs under the queue advisory lock of the caller.
UPDATE cloudiful_tasks.tasks
SET pause_state = CASE
        WHEN status = 'queued' THEN 'paused'
        ELSE 'pausing'
    END,
    pause_generation = pause_generation + 1,
    updated_at = clock_timestamp()
WHERE id = $3
  AND namespace = $1
  AND queue = $2
  AND status IN ('queued', 'running')
  AND pause_state = 'unpaused'
RETURNING *
