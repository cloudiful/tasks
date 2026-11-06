-- Resumes a pausing or paused task.
--
-- The available time and the attempt history are preserved, so a resume never
-- bypasses the retry backoff. The pause generation is retained as history; a
-- resume of an already unpaused task changes nothing.
UPDATE cloudiful_tasks.tasks
SET pause_state = 'unpaused',
    updated_at = clock_timestamp()
WHERE id = $3
  AND namespace = $1
  AND queue = $2
  AND status IN ('queued', 'running')
  AND pause_state IN ('pausing', 'paused')
RETURNING *
