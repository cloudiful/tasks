SELECT t.class, t.resources, t.attempt_count, a.fencing_token
FROM cloudiful_tasks.tasks AS t
INNER JOIN cloudiful_tasks.attempts AS a ON a.task_id = t.id
WHERE t.id = $1
