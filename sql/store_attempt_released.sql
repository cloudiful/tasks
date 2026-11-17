-- Whether the captured attempt was already released by its owner.
--
-- The task, the attempt, the fencing token, the worker of the attempt and the
-- identity of the caller identify the release, so a release of another
-- attempt and a replay of another worker never match.
SELECT EXISTS (
    SELECT 1
    FROM cloudiful_tasks.attempts AS a
    INNER JOIN cloudiful_tasks.tasks AS t ON t.id = a.task_id
    WHERE t.namespace = $1
      AND t.queue = $2
      AND a.task_id = $3
      AND a.id = $4
      AND a.fencing_token = $5
      AND a.worker_id = $6
      AND a.worker_id = $7
      AND a.status = 'released'
) AS released
