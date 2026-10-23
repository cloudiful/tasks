-- Takes the transaction advisory lock of one queue.
--
-- Claims and policy replacement use this lock, so a policy change and a
-- count-and-claim of the same queue cannot interleave. The two key form keeps
-- the lock separate from the one key migration lock space.
SELECT pg_advisory_xact_lock(
    hashtext('cloudiful_tasks.queue'),
    hashtext($1 || E'\x1f' || $2)
)
