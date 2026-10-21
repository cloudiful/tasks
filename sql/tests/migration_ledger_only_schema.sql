CREATE SCHEMA IF NOT EXISTS cloudiful_tasks;

CREATE TABLE IF NOT EXISTS cloudiful_tasks._sqlx_migrations (
    version bigint PRIMARY KEY,
    description text NOT NULL,
    installed_on timestamptz NOT NULL DEFAULT now(),
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL
);
