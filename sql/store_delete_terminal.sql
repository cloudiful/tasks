-- Deletes one terminal task of the queue.
--
-- The caller locks the task row before it reads the status, so a concurrent
-- retry or claim either completes before the check or waits until the row is
-- deleted. Only a terminal task of the namespace and queue is deleted; the
-- attempts of the task are removed by the ON DELETE CASCADE of the foreign
-- key.
DELETE FROM cloudiful_tasks.tasks
WHERE id = $3
  AND namespace = $1
  AND queue = $2
  AND status IN ('succeeded', 'failed', 'cancelled')
RETURNING id
