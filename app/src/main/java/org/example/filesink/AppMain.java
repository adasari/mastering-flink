package org.example.filesink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;


public class AppMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

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
        env.execute("Batch: Keyed File Sink with Index Example");
    }

    public static class KeyBucketAssigner implements BucketAssigner<String, String> {

        @Override
        public String getBucketId(String element, Context context) {
            return element.split(": ")[1];
        }

        @Override
        public SimpleVersionedSerializer<String> getSerializer() {
            return SimpleVersionedStringSerializer.INSTANCE;
        }
    }
}
