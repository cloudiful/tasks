WITH candidate AS (
    SELECT id, attempt_count
    FROM cloudiful_tasks.tasks
    WHERE namespace = $1
      AND queue = $2
      AND status = 'queued'
      AND available_at <= clock_timestamp()
      AND ($7::uuid IS NULL OR id = $7)
    ORDER BY priority DESC, available_at, created_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT 1
),
claimed AS (
    UPDATE cloudiful_tasks.tasks AS t
    SET status = 'running',
        attempt_count = t.attempt_count + 1,
        updated_at = clock_timestamp()
    FROM candidate AS c
    WHERE t.id = c.id
    RETURNING t.id
),
sealed_attempt AS (
    INSERT INTO cloudiful_tasks.attempts (
        id,
        task_id,
        attempt_number,
        worker_id,
        fencing_token,
        status,
        lease_expires_at,
        started_at
    )
    SELECT $4, c.id, c.attempt_count + 1, $3, $5, 'running',
           clock_timestamp() + ($6::bigint * interval '1 millisecond'), clock_timestamp()
    FROM candidate AS c
    RETURNING *
)
SELECT
    attempt.id,
    attempt.task_id,
    attempt.attempt_number,
    attempt.worker_id,
    attempt.fencing_token,
    attempt.status,
    attempt.lease_expires_at,
    attempt.started_at,
    attempt.finished_at,
    attempt.error,
    attempt.progress,
    attempt.progress_updated_at
FROM sealed_attempt AS attempt
INNER JOIN claimed ON claimed.id = attempt.task_id
