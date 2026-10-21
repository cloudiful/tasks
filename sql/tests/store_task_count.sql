SELECT count(*) AS task_count
FROM cloudiful_tasks.tasks
WHERE namespace = $1 AND queue = $2
