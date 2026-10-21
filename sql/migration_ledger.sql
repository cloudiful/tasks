SELECT version, checksum
FROM cloudiful_tasks._sqlx_migrations
WHERE success
ORDER BY version
