SELECT EXISTS (
    SELECT 1
    FROM cloudiful_tasks.attempts AS a
    WHERE a.task_id = $1
      AND a.status = 'running'
      AND a.lease_expires_at > now() - ($2::bigint * interval '1 microsecond')
) AS live
