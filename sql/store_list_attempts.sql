SELECT
    a.id,
    a.task_id,
    a.attempt_number,
    a.worker_id,
    a.fencing_token,
    a.status,
    a.lease_expires_at,
    a.started_at,
    a.finished_at,
    a.error,
    a.progress,
    a.progress_updated_at
FROM cloudiful_tasks.attempts AS a
INNER JOIN cloudiful_tasks.tasks AS t ON t.id = a.task_id
WHERE t.namespace = $1
  AND t.queue = $2
  AND a.task_id = $3
ORDER BY a.attempt_number DESC
LIMIT $4
