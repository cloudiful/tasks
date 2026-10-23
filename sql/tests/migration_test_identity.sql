SELECT current_database() AS database_name,
       current_user AS user_name,
       inet_server_addr()::text AS server_address,
       current_setting('port') AS server_port,
       shobj_description(
           (SELECT oid FROM pg_database WHERE datname = current_database()),
           'pg_database'
       ) AS database_comment
