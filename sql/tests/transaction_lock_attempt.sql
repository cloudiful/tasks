SELECT id
FROM cloudiful_tasks.attempts
WHERE id = $1
FOR UPDATE
