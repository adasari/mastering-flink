package org.example.processingtime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class App {
    @Getter
    @AllArgsConstructor
    @Setter
    @ToString
    static class TableRecord {
        private long sequenceId;
        private String table;
        private String name;
    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2000);
        GeneratorFunction<Long, TableRecord> tableAGeneratorFunction = index -> new TableRecord(index, "A", "Alice");
        GeneratorFunction<Long, TableRecord> tableBGeneratorFunction = index -> new TableRecord(index, "B", "Bob");
        long numberOfRecords = 100;
        DataGeneratorSource<TableRecord> sourceA =
                new DataGeneratorSource<>(
                        tableAGeneratorFunction,
                        numberOfRecords,
                        RateLimiterStrategy.perSecond(1),
                        Types.GENERIC(TableRecord.class));
        DataGeneratorSource<TableRecord> sourceB =
                new DataGeneratorSource<>(
                        tableBGeneratorFunction,
                        numberOfRecords,
                        RateLimiterStrategy.perSecond(1),
                        Types.GENERIC(TableRecord.class));

        DataStreamSource<TableRecord> tableADatastream =
                env.fromSource(sourceA,
                        WatermarkStrategy.noWatermarks(),
                        "Table A Generator Source",
                        Types.GENERIC(TableRecord.class)
                );

        DataStreamSource<TableRecord> tableBDatastream =
                env.fromSource(sourceB,
                        WatermarkStrategy.noWatermarks(),
                        "Table B Generator Source",
                        Types.GENERIC(TableRecord.class)
                );

        DataStream<TableRecord> inputDatastream = tableADatastream.union(tableBDatastream);
        inputDatastream.sinkTo(new DiscardingSink<>());

        env.execute("Processing time job");

    }
}
