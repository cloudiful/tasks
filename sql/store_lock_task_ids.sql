SELECT t.id
FROM cloudiful_tasks.tasks AS t
WHERE t.namespace = $1
  AND t.queue = $2
  AND t.id = ANY($3::uuid[])
ORDER BY t.id
FOR SHARE
