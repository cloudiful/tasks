SELECT resource, max_concurrent_attempts
FROM cloudiful_tasks.queue_resource_policies
WHERE namespace = $1 AND queue = $2
ORDER BY resource
