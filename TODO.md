Pending items:
1. Schema registry integration.
2. Debezium partition and other config verification.

Required improvements:
1. No table split support
2. StartOptions.snapshot() should stop the job when snapshot is completed ?

Further investigation:
1. Following postgres logs.:
   ```
    postgres-postgres-1  | 2024-10-09 03:42:29.700 UTC [202] LOG:  logical decoding found consistent point at 0/167E460
    postgres-postgres-1  | 2024-10-09 03:42:29.700 UTC [202] DETAIL:  There are no running transactions.
    postgres-postgres-1  | 2024-10-09 03:42:29.706 UTC [207] ERROR:  replication slot "flink_1" does not exist
    postgres-postgres-1  | 2024-10-09 03:42:29.706 UTC [207] STATEMENT:  select pg_drop_replication_slot('flink_1')
    postgres-postgres-1  | 2024-10-09 03:43:25.797 UTC [210] LOG:  starting logical decoding for slot "flink_0"
    postgres-postgres-1  | 2024-10-09 03:43:25.797 UTC [210] DETAIL:  Streaming transactions committing after 0/167E4F8, reading WAL from 0/167E4F8.
    postgres-postgres-1  | 2024-10-09 03:43:25.797 UTC [210] LOG:  logical decoding found consistent point at 0/167E4F8
    postgres-postgres-1  | 2024-10-09 03:43:25.797 UTC [210] DETAIL:  There are no running transactions.
    postgres-postgres-1  | 2024-10-09 03:43:59.080 UTC [213] ERROR:  replication slot "flink_1" does not exist
    postgres-postgres-1  | 2024-10-09 03:43:59.080 UTC [213] STATEMENT:  select pg_drop_replication_slot('flink_1')
    postgres-postgres-1  | 2024-10-09 03:44:03.190 UTC [215] ERROR:  replication slot "flink_0" does not exist
    postgres-postgres-1  | 2024-10-09 03:44:03.190 UTC [215] STATEMENT:  select pg_drop_replication_slot('flink_0')
    postgres-postgres-1  | 2024-10-09 03:44:09.848 UTC [217] ERROR:  replication slot "flink_0" does not exist
    postgres-postgres-1  | 2024-10-09 03:44:09.848 UTC [217] STATEMENT:  select pg_drop_replication_slot('flink_0')
    ```
