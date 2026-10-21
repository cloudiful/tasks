SELECT pg_advisory_lock($1::bigint)::text AS locked
