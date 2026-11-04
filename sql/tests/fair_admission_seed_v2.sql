-- Representative task and attempt of a schema before fair admission.
--
-- The task carries an idempotency key and the stored request specification of
-- the default request, so a replay after the upgrade must return this task.
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
    '11111111-1111-1111-1111-111111111111',
    'ns',
    'q',
    'demo',
    '{"legacy": true}',
    'running',
    0,
    now(),
    'legacy-key',
    '{"available_at": null, "kind": "demo", "payload": {"legacy": true}, "priority": 0, "retry": {"initial_delay_ns": 0, "max_attempts": 1, "max_delay_ns": 0}}',
    1,
    1,
    0,
    0
);

INSERT INTO cloudiful_tasks.attempts (
    id,
    task_id,
    attempt_number,
    worker_id,
    fencing_token,
    status,
    lease_expires_at,
    started_at
)
VALUES (
    '22222222-2222-2222-2222-222222222222',
    '11111111-1111-1111-1111-111111111111',
    1,
    'legacy-worker',
    '33333333-3333-3333-3333-333333333333',
    'running',
    now() + interval '1 hour',
    now()
);
