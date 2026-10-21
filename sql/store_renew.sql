UPDATE cloudiful_tasks.attempts AS a
SET lease_expires_at = clock_timestamp() + ($6::bigint * interval '1 millisecond')
FROM cloudiful_tasks.tasks AS t
WHERE t.id = $3
  AND t.namespace = $1
  AND t.queue = $2
  AND a.task_id = t.id
  AND a.id = $4
  AND a.fencing_token = $5
  AND a.status = 'running'
  AND a.lease_expires_at > clock_timestamp()
RETURNING a.lease_expires_at
