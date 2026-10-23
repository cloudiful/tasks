INSERT INTO cloudiful_tasks.queue_policies (
    namespace,
    queue,
    max_concurrent_attempts,
    updated_at
)
VALUES ($1, $2, $3, clock_timestamp())
ON CONFLICT (namespace, queue) DO UPDATE
SET max_concurrent_attempts = EXCLUDED.max_concurrent_attempts,
    updated_at = clock_timestamp()
