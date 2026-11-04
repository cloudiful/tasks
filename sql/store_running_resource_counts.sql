-- Resources of the queue whose running occupancy reached the policy limit.
--
-- One grouped aggregate joins the queue scoped policies, the immutable
-- resource keys of the tasks and the running attempts of the queue. Every
-- attempt counts once per declared key, including an attempt whose lease
-- expired but was not sealed by recovery yet. A resource without a policy row
-- is unlimited and never blocks a claim.
SELECT p.resource
FROM cloudiful_tasks.queue_resource_policies AS p
INNER JOIN cloudiful_tasks.tasks AS t
    ON t.namespace = p.namespace
    AND t.queue = p.queue
    AND t.resources @> ARRAY[p.resource]
INNER JOIN cloudiful_tasks.attempts AS a
    ON a.task_id = t.id
    AND a.status = 'running'
WHERE p.namespace = $1
  AND p.queue = $2
GROUP BY p.resource, p.max_concurrent_attempts
HAVING count(*) >= p.max_concurrent_attempts
ORDER BY p.resource
