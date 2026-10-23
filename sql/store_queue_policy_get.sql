SELECT max_concurrent_attempts
FROM cloudiful_tasks.queue_policies
WHERE namespace = $1 AND queue = $2
