-- Counts every running attempt of the queue, including an expired attempt
-- until recovery seals it.
SELECT count(*) AS running_attempts
FROM cloudiful_tasks.attempts AS a
INNER JOIN cloudiful_tasks.tasks AS t ON t.id = a.task_id
WHERE t.namespace = $1
  AND t.queue = $2
  AND a.status = 'running'
