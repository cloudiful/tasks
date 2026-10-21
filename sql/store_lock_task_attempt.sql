-- Locks the task and the attempt of a lease guarded operation.
--
-- The task is locked before the attempt, so that all operations of a task
-- take its locks in the same order. The locks are held while the lease of
-- the attempt is checked, so that the check uses the clock of the database
-- after a lock wait.
WITH task_guard AS (
    SELECT id, status
    FROM cloudiful_tasks.tasks
    WHERE id = $3 AND namespace = $1 AND queue = $2
    FOR UPDATE
),
attempt_guard AS (
    SELECT a.id
    FROM cloudiful_tasks.attempts AS a
    INNER JOIN task_guard ON task_guard.id = a.task_id
    WHERE a.id = $4
      AND a.fencing_token = $5
      AND a.status = 'running'
    FOR UPDATE OF a
)
SELECT
    (SELECT count(*) FROM task_guard) AS task_found,
    (SELECT count(*) FROM attempt_guard) AS attempt_found
