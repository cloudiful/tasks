-- Whether the captured attempt already holds a terminal seal.
--
-- The task, the attempt and the fencing token identify the attempt, so a
-- terminal of a newer attempt never matches.
SELECT EXISTS (
    SELECT 1
    FROM cloudiful_tasks.attempts AS a
    INNER JOIN cloudiful_tasks.tasks AS t ON t.id = a.task_id
    WHERE t.namespace = $1
      AND t.queue = $2
      AND a.task_id = $3
      AND a.id = $4
      AND a.fencing_token = $5
      AND a.status IN ('succeeded', 'failed', 'cancelled')
) AS sealed
