package org.example.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresParallelSourceExample {

    public static void main(String[] args) throws Exception {

        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema();

        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .startupOptions(StartupOptions.snapshot())
                        .hostname("localhost")
                        .port(5432)
                        .database("test")
                        .schemaList("public")
//                        .tableList("public.products")
                        .username("postgres")
                        .password("postgres")
                        .slotName("flink")
                        .decodingPluginName("pgoutput") // use pgoutput for PostgreSQL 10+
                        .deserializer(deserializer)
                        .splitSize(2) // the split size of each snapshot split
                        .build();

        // https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/state/task_failure_recovery/#restart-strategies
        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        config.setString("heartbeat.interval", "6000000"); // 100 minutes
        config.setString("heartbeat.timeout", "18000000");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.enableCheckpointing(300000);

        env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(2)
                .print();

        env.execute("Output Postgres Snapshot");
    }
}
