DROP SCHEMA IF EXISTS app_domain CASCADE;
CREATE SCHEMA app_domain;
CREATE TABLE app_domain.sentinel (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    note text NOT NULL
);
