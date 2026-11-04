DELETE FROM cloudiful_tasks.queue_resource_policies
WHERE namespace = $1 AND queue = $2 AND resource = $3
