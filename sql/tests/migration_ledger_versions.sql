SELECT version AS migration_version
FROM cloudiful_tasks._sqlx_migrations
WHERE success
ORDER BY version
