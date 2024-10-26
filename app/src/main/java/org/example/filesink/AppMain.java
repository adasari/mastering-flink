package org.example.filesink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;


public class AppMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(5000L);

        /*
        DataStream<String> input = env.fromElements(
                "apple", "banana", "apple", "orange", "banana", "apple"
        );

        DataStream<String> indexedStream = input
                .map(new RichMapFunction<String, String>() {
                    private int index = 0;

                    @Override
                    public String map(String value) throws Exception {
                        return (index++) + ": " + value;
                    }

//                    @Override
                    public void open(Configuration parameters) throws Exception {
                        index = 0;  // Ensure index starts at 0 for each task
                    }
                });

        // Key by the string value (without the index)
        DataStream<String> keyedStream = indexedStream
                .keyBy(value -> value.split(": ")[1]);  // Key by the original string (e.g., "apple", "banana")
         */

        // Set up the file sink with key-based buckets
//        final StreamingFileSink<String> sink = StreamingFileSink
//                .forRowFormat(new Path("file:///Users/ADASARI/work/learning/flink/flink-output"), new SimpleStringEncoder<String>("UTF-8"))
////                .withBucketAssigner(new BasePathBucketAssigner<>())
//                .withBucketAssigner(new KeyBucketAssigner())
//                .build();
//
//        // Write to file with key-based buckets
//        keyedStream.addSink(sink);

        // Execute the program


        GeneratorFunction<Long, String> tableAGeneratorFunction = index -> index+ "-A";
        long numberOfRecords = 50;
        DataGeneratorSource<String> sourceA =
                new DataGeneratorSource<>(
                        tableAGeneratorFunction,
                        numberOfRecords,
                        RateLimiterStrategy.perSecond(1),
                        Types.STRING);
        DataStreamSource<String> tableADatastream =
                env.fromSource(sourceA,
                        WatermarkStrategy.noWatermarks(),
                        "Table A Generator Source",
                        Types.STRING
                );
        DataStream<String> keyedStream = tableADatastream
                .keyBy(value -> value.split("-")[1]);
        org.apache.flink.connector.file.sink.FileSink<String> sink = org.apache.flink.connector.file.sink.FileSink.
                forRowFormat(new Path("file:///Users/ADASARI/work/learning/flink/flink-file-output"), new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new KeyBucketAssigner())
                .withBucketCheckInterval(5000L)
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(100))
//                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                        .build();
        keyedStream.sinkTo(sink);
        env.execute("Batch: Keyed File Sink with Index Example");
    }

    public static class KeyBucketAssigner implements BucketAssigner<String, String> {

        @Override
        public String getBucketId(String element, Context context) {
            return element.split("-")[1];
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
