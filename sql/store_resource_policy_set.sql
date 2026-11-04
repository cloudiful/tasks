INSERT INTO cloudiful_tasks.queue_resource_policies (
    namespace,
    queue,
    resource,
    max_concurrent_attempts,
    updated_at
)
VALUES ($1, $2, $3, $4, clock_timestamp())
ON CONFLICT (namespace, queue, resource) DO UPDATE
SET max_concurrent_attempts = EXCLUDED.max_concurrent_attempts,
    updated_at = clock_timestamp()
