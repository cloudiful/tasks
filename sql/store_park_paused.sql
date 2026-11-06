-- Seals the exact attempt of a task as a parked pause checkpoint.
--
-- The task and the attempt are locked by the caller in task then attempt
-- order. A running attempt that parked while a pause is still active leaves
-- the task queued and paused; a park that races a resume leaves the task
-- queued and unpaused so the continuation is claimable again. A cancellation
-- that was requested before the park wins. The attempt count already counts
-- the attempt, the park only adds it to the parked counter, so the effective
-- failure budget is not consumed.
WITH guard AS (
    SELECT id, status, pause_state
    FROM cloudiful_tasks.tasks
    WHERE id = $3 AND namespace = $1 AND queue = $2
),
sealed AS (
    UPDATE cloudiful_tasks.attempts AS a
    SET status = CASE
            WHEN guard.status = 'cancel_requested' THEN 'cancelled'
            ELSE 'paused'
        END,
        finished_at = clock_timestamp(),
        lease_expires_at = NULL,
        error = CASE
            WHEN guard.status = 'cancel_requested' THEN a.error
            ELSE NULL
        END,
        paused_at = CASE
            WHEN guard.status = 'cancel_requested' THEN NULL
            ELSE clock_timestamp()
        END
    FROM guard
    WHERE a.id = $4
      AND a.task_id = guard.id
      AND a.fencing_token = $5
      AND a.status = 'running'
      AND a.lease_expires_at > clock_timestamp()
    RETURNING a.task_id, (a.status = 'paused') AS paused
)
UPDATE cloudiful_tasks.tasks AS t
SET status = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        ELSE 'queued'
    END,
    pause_state = CASE
        WHEN t.status = 'cancel_requested' THEN 'unpaused'
        WHEN t.pause_state = 'unpaused' THEN 'unpaused'
        ELSE 'paused'
    END,
    paused_attempts = t.paused_attempts + CASE WHEN sealed.paused THEN 1 ELSE 0 END,
    finished_at = CASE
        WHEN t.status = 'cancel_requested' THEN clock_timestamp()
        ELSE NULL
    END,
    terminal_reason = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        ELSE NULL
    END,
    updated_at = clock_timestamp()
FROM sealed
WHERE t.id = sealed.task_id
RETURNING t.*
