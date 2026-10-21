INSERT INTO cloudiful_tasks.tasks (
    id,
    namespace,
    queue,
    kind,
    payload,
    status,
    priority,
    available_at,
    idempotency_key,
    request_spec,
    attempt_count,
    max_attempts,
    retry_initial_delay_ns,
    retry_max_delay_ns
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    'queued',
    $6,
    COALESCE($7::timestamptz, clock_timestamp()),
    $8,
    $9,
    0,
    $10,
    $11,
    $12
)
ON CONFLICT (namespace, queue, idempotency_key) WHERE idempotency_key IS NOT NULL
DO NOTHING
RETURNING id
