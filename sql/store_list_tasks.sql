SELECT *
FROM cloudiful_tasks.tasks
WHERE namespace = $1
  AND queue = $2
  AND (created_at, id) < ($3, $4)
ORDER BY created_at DESC, id DESC
LIMIT $5
