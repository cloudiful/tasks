SELECT last_class
FROM cloudiful_tasks.queue_class_cursors
WHERE namespace = $1 AND queue = $2
