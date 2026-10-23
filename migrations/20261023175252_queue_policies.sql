-- cloudiful-tasks schema, protocol version 2.
--
-- A queue policy stores the durable maximum number of running attempts of one
-- namespace and queue. A missing row means an unlimited queue.

CREATE TABLE cloudiful_tasks.queue_policies (
    namespace text NOT NULL,
    queue text NOT NULL,
    max_concurrent_attempts integer NOT NULL CHECK (max_concurrent_attempts > 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace, queue),
    CONSTRAINT queue_policies_namespace_check
        CHECK (namespace ~ '^[-A-Za-z0-9_.:]{1,128}$'),
    CONSTRAINT queue_policies_queue_check
        CHECK (queue ~ '^[-A-Za-z0-9_.:]{1,128}$')
);

UPDATE cloudiful_tasks.schema_ownership
SET protocol_version = 2,
    updated_at = now()
WHERE singleton;
