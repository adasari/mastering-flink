package org.example.cdc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class PostgresToKafkaExample {

    public static void main(String[] args) throws Exception {

        DebeziumDeserializationSchema<String> deserializer =
                new JsonDebeziumDeserializationSchema(true);

        Properties properties = new Properties();
        properties.setProperty("connector.class", "test.connector.PostgresConnector");
        properties.setProperty("debezium.slot.drop.on.stop", "false");
        properties.setProperty("slot.drop.on.stop", "false");
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .startupOptions(StartupOptions.snapshot())
                        .hostname("localhost")
                        .port(5432)
                        .database("test")
                        .schemaList("public")
                        .username("postgres")
                        .password("postgres")
                        .slotName("backfillslottest04")
                        .decodingPluginName("pgoutput")
                        .deserializer(deserializer)
                        .debeziumProperties(properties)
                        .skipSnapshotBackfill(true)
                        .splitSize(1)
                        .build();

        Configuration config = new Configuration();
        config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
        config.setString("heartbeat.interval", "6000000"); // 100 minutes
        config.setString("heartbeat.timeout", "18000000");
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:/home/work/learning/flink/checkpoint");
        config.set(CoreOptions.DEFAULT_PARALLELISM, 1);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(config);
        env.enableCheckpointing(2000);

        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .uid("source2")
                .setParallelism(1);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9093")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test-topic")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .setKeySerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setProperty("auto.create.topics.enable", "true")
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        stringSingleOutputStreamOperator.sinkTo(sink);

        env.execute("Output Postgres Snapshot");
    }
}
