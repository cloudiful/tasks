SELECT id
FROM cloudiful_tasks.tasks
WHERE id = $1
FOR UPDATE
