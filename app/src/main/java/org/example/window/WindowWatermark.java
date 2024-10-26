package org.example.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.example.processingtime.DiscardingSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WindowWatermark {
    private static final Logger logger = LoggerFactory.getLogger(WindowWatermark.class);

    @Data
    @Getter
    @Setter
    @AllArgsConstructor
    static class Record {
        private String record;
        private long windowStart;
        private long windowEnd;
        private long processingTime;

    }
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.enableCheckpointing(5000L);
        env.getConfig().setAutoWatermarkInterval(10000);

        GeneratorFunction<Long, String> tableAGeneratorFunction = index -> index+ "-A" + "-"+System.currentTimeMillis();
        long numberOfRecords = 35;
        DataGeneratorSource<String> sourceA =
                new DataGeneratorSource<>(
                        tableAGeneratorFunction,
                        numberOfRecords,
                        RateLimiterStrategy.perSecond(1),
                        Types.STRING);
        DataStreamSource<String> tableADatastream =
                env.fromSource(sourceA,
                        WatermarkStrategy.forGenerator((WatermarkGeneratorSupplier<String>) context -> new PeriodicWatermarkGenerator<>()),
//                        WatermarkStrategy.
//                                <String>forBoundedOutOfOrderness(Duration.ofSeconds(15))
//                                .withTimestampAssigner(
//                                        (SerializableTimestampAssigner<String>)
//                                                (event, timestamp) -> timestamp
//                                )
//                                .withIdleness(Duration.ofSeconds(5)),
                        "Table A Generator Source",
                        Types.STRING
                );

        tableADatastream
//                .windowAll(TumblingProcessingTimeWindows.of(Duration.ofSeconds(30)))
//                .trigger(new CustomProcessingTimeTrigger())
//                .process(new ProcessAllWindowFunction<String, Record, TimeWindow>() {
//                    @Override
//                    public void process(ProcessAllWindowFunction<String, Record, TimeWindow>.Context context, Iterable<String> iterable, Collector<Record> collector) throws Exception {
//                        iterable.forEach(s -> {
//                            Record r = new Record(s, context.window().getStart(), context.window().getEnd(), System.currentTimeMillis());
//                            collector.collect(r);
//                        });
//                    }
//                })
                .sinkTo(new DiscardingSink<>());

        env.execute("Batch: Keyed File Sink with Index Example");
    }

}
