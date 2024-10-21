package org.example.customsink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class App {

    static class InputRecord {
        private long sequenceId;
        private String table;

        public InputRecord(long sequenceId, String value) {
            this.sequenceId = sequenceId;
            this.table = value;
        }

        public String getTable() {
            return table;
        }

        public long getSequenceId() {
            return sequenceId;
        }

        public void setSequenceId(long sequenceId) {
            this.sequenceId = sequenceId;
        }

        public void setTable(String table) {
            this.table = table;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(3);
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.enableCheckpointing(2000);
        GeneratorFunction<Long, InputRecord> generatorFunction = index -> {
            int remainder = (int) (index % 10);
            String value = "";
            switch (remainder) {
                case 0:
                case 5:
                    value = "A";
                    break;
                case 1:
                case 6:
                    value = "B";
                    break;
                case 2:
                case 7:
                    value = "C";
                    break;
                case 3:
                case 8:
                    value = "D";
                    break;
                case 4:
                case 9:
                    value = "E";
                    break;
            }

            return new InputRecord(index, value);
        };
        long numberOfRecords = 20;
        DataGeneratorSource<InputRecord> source =
                new DataGeneratorSource<>(
                        generatorFunction,
                        numberOfRecords,
                        RateLimiterStrategy.perSecond(1),
                        Types.GENERIC(InputRecord.class));

        DataStreamSource<InputRecord> dataStream =
                env.fromSource(source,
                                WatermarkStrategy.noWatermarks(),
                                "Generator Source",
                                Types.GENERIC(InputRecord.class)
                        );

        dataStream.keyBy(InputRecord::getTable)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .trigger(ProcessingTimeTrigger.create())
                .process(new ProcessWindowFunction<InputRecord, Record, String, TimeWindow>() {
                    /**
                     * Returns Record object i.e. input string and window timestamp.
                     */
                    @Override
                    public void process(String key, ProcessWindowFunction<InputRecord, Record, String, TimeWindow>.Context context, Iterable<InputRecord> iterable, Collector<Record> collector) throws Exception {
                        iterable.forEach(e -> {
                            collector.collect(new Record(e.getTable(), context.window().getStart(), e.getSequenceId()));
                        });
                    }
                })
                .sinkTo(new CustomSink("/Users/ADASARI/work/workspace/scala/mastering-flink/out"));

        env.execute("Two phase commit job");
    }
}
