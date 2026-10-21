SELECT count(*) AS lock_count
FROM pg_catalog.pg_locks
WHERE locktype = 'advisory'
