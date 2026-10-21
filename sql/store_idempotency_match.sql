SELECT id, (request_spec = $4::jsonb) AS spec_matches
FROM cloudiful_tasks.tasks
WHERE namespace = $1 AND queue = $2 AND idempotency_key = $3
