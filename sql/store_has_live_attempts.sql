SELECT EXISTS (
    SELECT 1
    FROM cloudiful_tasks.attempts AS a
    INNER JOIN cloudiful_tasks.tasks AS t
        ON t.id = a.task_id
    WHERE t.namespace = $1
      AND t.queue = $2
      AND a.task_id = ANY($3::uuid[])
      AND a.status = 'running'
      AND a.lease_expires_at > now()
) AS live
