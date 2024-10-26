package org.example.window;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;


public class AppMain {
    private static final Logger logger = LoggerFactory.getLogger(AppMain.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(5000L);

        GeneratorFunction<Long, String> tableAGeneratorFunction = index -> index+ "-A" + "-"+System.currentTimeMillis();
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

        tableADatastream
//                .keyBy(s -> s.split("-")[0])
                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(30)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<String, String, TimeWindow>.Context context, Iterable<String> iterable, Collector<String> collector) throws Exception {
                        iterable.forEach(s -> {
                            logger.info("processing element {} at {} - {}", s, System.currentTimeMillis(), context.window().getStart());
                            collector.collect(s);
                        });
                    }
                })
                .sinkTo(new DiscardingSink<>());


//        tableADatastream
//                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
//                .sum("1");
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
