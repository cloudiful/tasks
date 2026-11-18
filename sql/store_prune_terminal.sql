-- statement_timestamp() is stable per statement and keeps finished_at on the index condition.
WITH candidate AS (
    SELECT t.id, t.finished_at
    FROM cloudiful_tasks.tasks AS t
    WHERE t.namespace = $1
      AND t.queue = $2
      AND t.finished_at IS NOT NULL
      AND (
          ($3::bigint IS NOT NULL
              AND t.status = 'succeeded'
              AND t.finished_at <= statement_timestamp() - ($3::bigint * interval '1 microsecond'))
          OR ($4::bigint IS NOT NULL
              AND t.status = 'failed'
              AND t.finished_at <= statement_timestamp() - ($4::bigint * interval '1 microsecond'))
          OR ($5::bigint IS NOT NULL
              AND t.status = 'cancelled'
              AND t.finished_at <= statement_timestamp() - ($5::bigint * interval '1 microsecond'))
      )
    ORDER BY t.finished_at, t.id
    LIMIT $6
    FOR UPDATE SKIP LOCKED
),
deleted AS (
    DELETE FROM cloudiful_tasks.tasks AS t
    USING candidate
    WHERE t.id = candidate.id
    RETURNING t.id, t.finished_at
)
SELECT id
FROM deleted
ORDER BY finished_at, id
