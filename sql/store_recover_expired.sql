WITH expired AS (
    SELECT
        t.id AS task_id,
        a.id AS attempt_id,
        a.attempt_number,
        a.fencing_token
    FROM cloudiful_tasks.tasks AS t
    INNER JOIN cloudiful_tasks.attempts AS a
        ON a.task_id = t.id
        AND a.status = 'running'
        AND a.lease_expires_at <= clock_timestamp()
    WHERE t.namespace = $1 AND t.queue = $2
    ORDER BY a.lease_expires_at, a.id
    FOR UPDATE OF t, a SKIP LOCKED
    LIMIT $3
),
sealed AS (
    UPDATE cloudiful_tasks.attempts AS a
    SET status = 'interrupted',
        finished_at = clock_timestamp(),
        lease_expires_at = NULL,
        error = jsonb_build_object('reason', 'lease_expired')
    FROM expired
    WHERE a.id = expired.attempt_id
      AND a.status = 'running'
      AND a.fencing_token = expired.fencing_token
    RETURNING a.task_id, expired.attempt_number
)
UPDATE cloudiful_tasks.tasks AS t
SET status = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        WHEN t.attempt_count < t.max_attempts THEN 'queued'
        ELSE 'failed'
    END,
    available_at = CASE
        WHEN t.status = 'cancel_requested' THEN t.available_at
        WHEN t.attempt_count < t.max_attempts THEN clock_timestamp() + make_interval(
            secs => LEAST(
                t.retry_initial_delay_ns::numeric
                    * power(2::numeric, LEAST(GREATEST(sealed.attempt_number - 1, 0), 31)),
                t.retry_max_delay_ns::numeric
            ) / 1000000000.0
        )
        ELSE t.available_at
    END,
    finished_at = CASE
        WHEN t.status = 'cancel_requested' OR t.attempt_count >= t.max_attempts THEN clock_timestamp()
        ELSE t.finished_at
    END,
    terminal_reason = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        WHEN t.attempt_count >= t.max_attempts THEN 'attempts_exhausted'
        ELSE t.terminal_reason
    END,
    updated_at = clock_timestamp()
FROM sealed
WHERE t.id = sealed.task_id
  AND t.status IN ('running', 'cancel_requested')
RETURNING t.*
