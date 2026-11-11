-- A task of an older build, whose persisted request specification holds the
-- fixed six microsecond representation of the availability.
--
-- The row is written directly, so the test proves that a specification of an
-- older build is still the same request.
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
    '00000000-0000-0000-0000-000000000004',
    'ns',
    'q',
    'demo',
    '{"a": 1}',
    'queued',
    0,
    '2024-01-02T03:04:05.123456Z',
    'legacy-key',
    '{"available_at": "2024-01-02T03:04:05.123456Z", "kind": "demo", "payload": {"a": 1}, "priority": 0, "retry": {"initial_delay_ns": 0, "max_attempts": 1, "max_delay_ns": 0}}',
    0,
    1,
    0,
    0
)
