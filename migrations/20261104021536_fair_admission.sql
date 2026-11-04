ALTER TABLE cloudiful_tasks.tasks
    ADD COLUMN class text,
    ADD COLUMN resources text[] NOT NULL DEFAULT '{}',
    ADD CONSTRAINT tasks_class_check
        CHECK (class IS NULL OR class ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    ADD CONSTRAINT tasks_resources_bound_check
        CHECK (cardinality(resources) <= 16 AND array_position(resources, NULL) IS NULL);

CREATE INDEX tasks_claim_class_idx
    ON cloudiful_tasks.tasks (namespace, queue, class, priority DESC, available_at, created_at, id)
    WHERE status = 'queued';

CREATE INDEX tasks_resources_gin ON cloudiful_tasks.tasks USING gin (resources);

CREATE TABLE cloudiful_tasks.queue_class_cursors (
    namespace text NOT NULL CHECK (namespace ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    queue text NOT NULL CHECK (queue ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    last_class text NOT NULL,
    turn bigint NOT NULL DEFAULT 0 CHECK (turn >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, queue),
    CHECK (last_class = '' OR last_class ~ '^[-A-Za-z0-9_.:]{1,128}$')
);

CREATE TABLE cloudiful_tasks.queue_resource_policies (
    namespace text NOT NULL CHECK (namespace ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    queue text NOT NULL CHECK (queue ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    resource text NOT NULL CHECK (resource ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    max_concurrent_attempts integer NOT NULL CHECK (max_concurrent_attempts > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, queue, resource)
);

UPDATE cloudiful_tasks.schema_ownership
SET protocol_version = 3,
    updated_at = now()
WHERE singleton;
