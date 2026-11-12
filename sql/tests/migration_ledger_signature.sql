SELECT
    version,
    description,
    success,
    encode(checksum, 'hex') AS checksum_hex,
    execution_time,
    installed_on::text AS installed_on
FROM cloudiful_tasks._sqlx_migrations
ORDER BY version
