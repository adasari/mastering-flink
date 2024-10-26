package org.example.customfilesink.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.RollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.example.customfilesink.FileSink;

import java.io.File;
import java.io.IOException;


public class App {

    public static class KeyBucketAssigner implements BucketAssigner<TableRecord, String> {

        @Override
        public String getBucketId(TableRecord tableRecord, Context context) {
            return tableRecord.getTable() + File.separator + String.valueOf(context.currentProcessingTime());
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
//        env.enableCheckpointing(100000000);
        GeneratorFunction<Long, TableRecord> tableAGeneratorFunction = index -> new TableRecord(index, "A", "Alice");
        GeneratorFunction<Long, TableRecord> tableBGeneratorFunction = index -> new TableRecord(index, "B", "Bob");
        long numberOfRecords = 20;
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

        Encoder<TableRecord> encoder = (tableRecord, stream) -> {
            stream.write(tableRecord.toString().getBytes());
            stream.write(System.lineSeparator().getBytes());
        };

        Sink<TableRecord> sink = FileSink.forRowFormat(
                        new Path("file:///Users/ADASARI/work/learning/flink/flink-output"),
                        encoder
                )
                .disableCompact()
                .withBucketAssigner(new KeyBucketAssigner())
                .withBucketCheckInterval(5000L)
                .withOutputFileConfig(OutputFileConfig.builder().withPartPrefix("part").build())
                .withRollingPolicy(new RollingPolicy<TableRecord, String>() {
                    @Override
                    public boolean shouldRollOnCheckpoint(PartFileInfo<String> partFileInfo) throws IOException {
                        return false;
                    }

                    @Override
                    public boolean shouldRollOnEvent(PartFileInfo<String> partFileInfo, TableRecord tableRecord) throws IOException {
                        return false;
                    }

                    @Override
                    public boolean shouldRollOnProcessingTime(PartFileInfo<String> partFileInfo, long l) throws IOException {
                        return true;
                    }
                })
                .build();


        tableADatastream.sinkTo(sink);
        env.execute("Two phase commit job");
    }
}
