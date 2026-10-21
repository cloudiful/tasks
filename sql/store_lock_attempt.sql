-- Locks the attempt of a lease guarded operation.
--
-- The lock is held while the lease of the attempt is checked, so that the
-- check uses the clock of the database after a lock wait. The task of the
-- attempt is only checked, it is not locked.
WITH attempt_guard AS (
    SELECT a.id
    FROM cloudiful_tasks.attempts AS a
    INNER JOIN cloudiful_tasks.tasks AS t ON t.id = a.task_id
    WHERE t.id = $3
      AND t.namespace = $1
      AND t.queue = $2
      AND a.id = $4
      AND a.fencing_token = $5
      AND a.status = 'running'
    FOR UPDATE OF a
)
SELECT
    (SELECT count(*)
     FROM cloudiful_tasks.tasks
     WHERE id = $3 AND namespace = $1 AND queue = $2) AS task_found,
    (SELECT count(*) FROM attempt_guard) AS attempt_found
