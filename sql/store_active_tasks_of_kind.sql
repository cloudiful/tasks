WITH active AS (
    SELECT t.id
    FROM cloudiful_tasks.tasks AS t
    WHERE t.namespace = $1
      AND t.queue = $2
      AND t.kind = $3
      AND (
          t.status IN ('queued', 'running', 'cancel_requested')
          OR EXISTS (
              SELECT 1
              FROM cloudiful_tasks.attempts AS a
              WHERE a.task_id = t.id
                AND a.status = 'running'
                AND a.lease_expires_at > now() - ($4::bigint * interval '1 microsecond')
          )
      )
    ORDER BY t.id
    LIMIT 513
)
SELECT t.*
FROM cloudiful_tasks.tasks AS t
INNER JOIN active ON active.id = t.id
ORDER BY t.id
