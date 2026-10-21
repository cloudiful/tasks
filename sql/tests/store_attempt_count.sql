SELECT count(*) AS attempt_count
FROM cloudiful_tasks.attempts
WHERE task_id = $1
