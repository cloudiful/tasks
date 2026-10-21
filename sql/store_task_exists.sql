SELECT EXISTS (
    SELECT 1
    FROM cloudiful_tasks.tasks
    WHERE id = $3 AND namespace = $1 AND queue = $2
)
