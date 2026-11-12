INSERT INTO cloudiful_tasks.tasks (
    id, namespace, queue, kind, payload, status, request_spec,
    attempt_count, max_attempts, pause_state, pause_generation, paused_attempts
)
VALUES (
    '11111111-1111-4111-8111-111111111111', 'legacy', 'q', 'demo',
    '{"stage": "paused"}', 'queued', '{"kind": "demo"}',
    1, 3, 'paused', 2, 1
);

INSERT INTO cloudiful_tasks.tasks (
    id, namespace, queue, kind, payload, status, request_spec,
    attempt_count, max_attempts
)
VALUES (
    '22222222-2222-4222-8222-222222222222', 'legacy', 'q', 'demo',
    '{"stage": "ready"}', 'queued', '{"kind": "demo"}',
    0, 3
);

INSERT INTO cloudiful_tasks.tasks (
    id, namespace, queue, kind, payload, status, request_spec,
    attempt_count, max_attempts, pause_state, pause_generation
)
VALUES (
    '33333333-3333-4333-8333-333333333333', 'legacy', 'q', 'demo',
    '{"stage": "running"}', 'running', '{"kind": "demo"}',
    1, 3, 'pausing', 1
);

INSERT INTO cloudiful_tasks.attempts (
    id, task_id, attempt_number, worker_id, fencing_token, status,
    lease_expires_at, pause_generation
)
VALUES (
    '44444444-4444-4444-8444-444444444444',
    '33333333-3333-4333-8333-333333333333',
    1, 'legacy-worker', '55555555-5555-4555-8555-555555555555', 'running',
    now() + interval '30 minutes', 1
);

INSERT INTO cloudiful_tasks.queue_policies (namespace, queue, max_concurrent_attempts)
VALUES ('legacy', 'q', 3);
