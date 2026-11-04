-- Advances the fair admission cursor after a granted candidate.
--
-- The cursor only moves when a claim was granted. An empty class is the
-- default class of a task without a class.
INSERT INTO cloudiful_tasks.queue_class_cursors (
    namespace,
    queue,
    last_class,
    turn,
    updated_at
)
VALUES ($1, $2, $3, 1, clock_timestamp())
ON CONFLICT (namespace, queue) DO UPDATE
SET last_class = EXCLUDED.last_class,
    turn = cloudiful_tasks.queue_class_cursors.turn + 1,
    updated_at = clock_timestamp()
