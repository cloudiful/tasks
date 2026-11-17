-- Releases the exact attempt of a task without spending its failure budget.
--
-- The task and the attempt are locked by the caller in task then attempt
-- order. Only the attempt that the caller owns is released: the namespace,
-- the queue, the task, the attempt, the fencing token, the worker of the
-- attempt and the identity of the caller must match, the attempt must still
-- run and its lease must not have expired. A cancellation that was requested
-- before the release wins and seals the attempt as cancelled, a task that
-- waits for a cooperative pause stays paused and unclaimable.
--
-- The attempt count already counts the attempt, the release only adds it to
-- the released counter, so the effective failure budget of the task does not
-- change. The task becomes queued and immediately claimable, without a
-- backoff.
WITH guard AS (
    SELECT id, status, pause_state
    FROM cloudiful_tasks.tasks
    WHERE id = $3 AND namespace = $1 AND queue = $2
),
sealed AS (
    UPDATE cloudiful_tasks.attempts AS a
    SET status = CASE
            WHEN guard.status = 'cancel_requested' THEN 'cancelled'
            ELSE 'released'
        END,
        finished_at = clock_timestamp(),
        lease_expires_at = NULL,
        error = NULL,
        paused_at = NULL
    FROM guard
    WHERE a.id = $4
      AND a.task_id = guard.id
      AND a.fencing_token = $5
      AND a.worker_id = $6
      AND a.worker_id = $7
      AND a.status = 'running'
      AND a.lease_expires_at > clock_timestamp()
    RETURNING a.task_id, (a.status = 'released') AS released
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
    released_attempts = t.released_attempts + CASE WHEN sealed.released THEN 1 ELSE 0 END,
    available_at = CASE
        WHEN t.status = 'cancel_requested' THEN t.available_at
        ELSE clock_timestamp()
    END,
    result = NULL,
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
