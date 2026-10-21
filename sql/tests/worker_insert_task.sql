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
    gen_random_uuid(),
    'ns',
    'q',
    'demo',
    '{"value": 1}',
    'queued',
    0,
    now(),
    NULL,
    '{"kind": "demo", "payload": {"value": 1}, "priority": 0}',
    0,
    1,
    0,
    0
)
