CREATE TABLE IF NOT EXISTS public.sentinel_ledger (
    version bigint PRIMARY KEY,
    description text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public._sqlx_migrations (
    version bigint PRIMARY KEY,
    description text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now()
);
