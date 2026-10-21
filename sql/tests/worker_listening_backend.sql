SELECT pid
FROM pg_catalog.pg_stat_activity
WHERE datname = current_database()
  AND state = 'idle'
  AND query ILIKE 'LISTEN%'
