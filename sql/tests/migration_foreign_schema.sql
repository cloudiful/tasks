CREATE SCHEMA IF NOT EXISTS cloudiful_tasks;

CREATE TABLE IF NOT EXISTS cloudiful_tasks.foreign_tasks (
    id uuid PRIMARY KEY,
    payload text NOT NULL
);
