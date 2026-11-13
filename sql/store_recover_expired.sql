WITH expired AS (
    SELECT
        t.id AS task_id,
        a.id AS attempt_id,
        a.attempt_number,
        a.fencing_token,
        (t.status = 'cancel_requested') AS cancel_requested,
        (
            t.pause_state IN ('pausing', 'paused')
            AND t.pause_generation > a.pause_generation
        ) AS pause_active
    FROM cloudiful_tasks.tasks AS t
    INNER JOIN cloudiful_tasks.attempts AS a
        ON a.task_id = t.id
        AND a.status = 'running'
        AND a.lease_expires_at <= clock_timestamp()
    WHERE t.namespace = $1 AND t.queue = $2
      AND ($4::uuid IS NULL OR t.id = $4)
    ORDER BY a.lease_expires_at, a.id
    FOR UPDATE OF t, a SKIP LOCKED
    LIMIT $3
),
sealed AS (
    UPDATE cloudiful_tasks.attempts AS a
    SET status = CASE
            WHEN expired.cancel_requested THEN 'cancelled'
            WHEN expired.pause_active THEN 'paused'
            ELSE 'interrupted'
        END,
        finished_at = clock_timestamp(),
        lease_expires_at = NULL,
        error = CASE
            WHEN expired.cancel_requested OR expired.pause_active THEN NULL
            ELSE jsonb_build_object('reason', 'lease_expired')
        END,
        paused_at = CASE
            WHEN expired.pause_active AND NOT expired.cancel_requested THEN clock_timestamp()
            ELSE NULL
        END
    FROM expired
    WHERE a.id = expired.attempt_id
      AND a.status = 'running'
      AND a.fencing_token = expired.fencing_token
    RETURNING
        a.task_id,
        expired.attempt_number,
        expired.cancel_requested,
        expired.pause_active
)
UPDATE cloudiful_tasks.tasks AS t
SET status = CASE
        WHEN sealed.cancel_requested THEN 'cancelled'
        WHEN sealed.pause_active THEN 'queued'
        WHEN (t.attempt_count - t.paused_attempts) < t.max_attempts THEN 'queued'
        ELSE 'failed'
    END,
    pause_state = CASE
        WHEN sealed.cancel_requested THEN 'unpaused'
        WHEN sealed.pause_active THEN 'paused'
        WHEN (t.attempt_count - t.paused_attempts) < t.max_attempts
            AND t.pause_state <> 'unpaused' THEN 'paused'
        ELSE 'unpaused'
    END,
    paused_attempts = t.paused_attempts + CASE
        WHEN sealed.pause_active AND NOT sealed.cancel_requested THEN 1
        ELSE 0
    END,
    available_at = CASE
        WHEN t.status = 'cancel_requested' THEN t.available_at
        WHEN sealed.pause_active THEN t.available_at
        WHEN (t.attempt_count - t.paused_attempts) < t.max_attempts THEN clock_timestamp() + make_interval(
            secs => LEAST(
                t.retry_initial_delay_ns::numeric
                    * power(2::numeric, LEAST(GREATEST(sealed.attempt_number - 1, 0), 31)),
                t.retry_max_delay_ns::numeric
            ) / 1000000000.0
        )
        ELSE t.available_at
    END,
    finished_at = CASE
        WHEN t.status = 'cancel_requested' THEN clock_timestamp()
        WHEN sealed.pause_active THEN NULL
        WHEN (t.attempt_count - t.paused_attempts) >= t.max_attempts THEN clock_timestamp()
        ELSE t.finished_at
    END,
    terminal_reason = CASE
        WHEN t.status = 'cancel_requested' THEN 'cancelled'
        WHEN sealed.pause_active THEN NULL
        WHEN (t.attempt_count - t.paused_attempts) >= t.max_attempts THEN 'attempts_exhausted'
        ELSE t.terminal_reason
    END,
    updated_at = clock_timestamp()
FROM sealed
WHERE t.id = sealed.task_id
  AND t.status IN ('running', 'cancel_requested')
RETURNING t.*
