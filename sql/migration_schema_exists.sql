SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_namespace
    WHERE nspname = $1
) AS schema_exists
