-- cloudiful-tasks schema, protocol version 1.
--
-- The schema owns the task table, the attempt table and the ownership marker
-- that identifies the library and the schema instance.

CREATE SCHEMA IF NOT EXISTS cloudiful_tasks;

CREATE TABLE cloudiful_tasks.schema_ownership (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    library text NOT NULL,
    protocol_version integer NOT NULL CHECK (protocol_version >= 1),
    instance_uuid uuid NOT NULL UNIQUE,
    installed_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE cloudiful_tasks.tasks (
    id uuid PRIMARY KEY,
    namespace text NOT NULL,
    queue text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL,
    result jsonb,
    status text NOT NULL,
    priority integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    idempotency_key text,
    request_spec jsonb NOT NULL,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts integer NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
    retry_initial_delay_ns bigint NOT NULL DEFAULT 0 CHECK (retry_initial_delay_ns >= 0),
    retry_max_delay_ns bigint NOT NULL DEFAULT 0 CHECK (retry_max_delay_ns >= 0),
    cancel_requested_at timestamptz,
    finished_at timestamptz,
    terminal_reason text,
    CONSTRAINT tasks_status_check
        CHECK (status IN ('queued', 'running', 'cancel_requested', 'succeeded', 'failed', 'cancelled')),
    CONSTRAINT tasks_namespace_check
        CHECK (namespace ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    CONSTRAINT tasks_queue_check
        CHECK (queue ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    CONSTRAINT tasks_kind_check
        CHECK (kind ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    CONSTRAINT tasks_idempotency_key_check
        CHECK (idempotency_key IS NULL OR char_length(idempotency_key) BETWEEN 1 AND 255)
);

CREATE UNIQUE INDEX tasks_idempotency_key_unique
    ON cloudiful_tasks.tasks (namespace, queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX tasks_claim_idx
    ON cloudiful_tasks.tasks (namespace, queue, priority DESC, available_at, created_at, id)
    WHERE status = 'queued';

CREATE INDEX tasks_namespace_queue_idx
    ON cloudiful_tasks.tasks (namespace, queue, created_at DESC, id DESC);

CREATE TABLE cloudiful_tasks.attempts (
    id uuid PRIMARY KEY,
    task_id uuid NOT NULL REFERENCES cloudiful_tasks.tasks (id) ON DELETE CASCADE,
    attempt_number integer NOT NULL CHECK (attempt_number >= 1),
    worker_id text NOT NULL CHECK (char_length(worker_id) BETWEEN 1 AND 128),
    fencing_token uuid NOT NULL UNIQUE,
    status text NOT NULL,
    lease_expires_at timestamptz,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    error jsonb,
    progress jsonb,
    progress_updated_at timestamptz,
    CONSTRAINT attempts_status_check
        CHECK (status IN ('running', 'succeeded', 'failed', 'cancelled', 'interrupted')),
    CONSTRAINT attempts_lease_check
        CHECK ((status = 'running' AND lease_expires_at IS NOT NULL)
            OR (status <> 'running' AND lease_expires_at IS NULL)),
    CONSTRAINT attempts_task_number_unique UNIQUE (task_id, attempt_number)
);

CREATE UNIQUE INDEX attempts_one_active_per_task
    ON cloudiful_tasks.attempts (task_id)
    WHERE status = 'running';

CREATE INDEX attempts_lease_idx
    ON cloudiful_tasks.attempts (status, lease_expires_at);

INSERT INTO cloudiful_tasks.schema_ownership (singleton, library, protocol_version, instance_uuid)
VALUES (true, 'cloudiful-tasks', 1, gen_random_uuid());
