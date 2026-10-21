UPDATE cloudiful_tasks._sqlx_migrations
SET checksum = decode('00', 'hex')
WHERE success;
