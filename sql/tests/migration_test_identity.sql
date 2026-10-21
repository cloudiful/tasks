SELECT current_database() AS database_name,
       current_user AS user_name,
       inet_server_addr()::text AS server_address,
       current_setting('port') AS server_port,
       current_setting('unix_socket_directories') AS socket_directories
