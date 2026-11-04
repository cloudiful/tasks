-- Ordered candidates of a fair claim.
--
-- The rotation starts with the classes above the last granted class in
-- ascending order and wraps to the remaining classes. Inside one class the
-- current priority order applies. The default class is the empty string.
-- Tasks that declare a blocked shared resource are skipped, so a blocked head
-- never hides an admissible candidate of the same class or of a later class.
SELECT t.id
FROM cloudiful_tasks.tasks AS t
WHERE t.namespace = $1
  AND t.queue = $2
  AND t.id = ANY($3::uuid[])
  AND t.status = 'queued'
  AND t.available_at <= clock_timestamp()
  AND NOT (t.resources && $4::text[])
ORDER BY
    CASE WHEN COALESCE(t.class, '') > $5 THEN 0 ELSE 1 END,
    COALESCE(t.class, ''),
    t.priority DESC,
    t.available_at,
    t.created_at,
    t.id
LIMIT 1000
