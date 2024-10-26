package org.example.processingtime;

import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.example.customsink.GenericObjectSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DiscardingSink<IN> implements Sink<IN>, SupportsConcurrentExecutionAttempts, ProcessingTimeService.ProcessingTimeCallback, SupportsWriterState<IN, Void> {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(DiscardingSink.class);

    public DiscardingSink() {
    }

    public SinkWriter<IN> createWriter(WriterInitContext context) throws IOException {
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public void onProcessingTime(long l) throws IOException, InterruptedException, Exception {
        logger.info("**** DiscardingSink onProcessingTime **** {}", l);
    }

    @Override
    public StatefulSinkWriter<IN, Void> restoreWriter(WriterInitContext writerInitContext, Collection<Void> collection) throws IOException {
        DiscardingElementWriter<IN> writer = new DiscardingElementWriter<>(writerInitContext.getProcessingTimeService());
        writer.initializeState();
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<Void> getWriterStateSerializer() {
        return new GenericObjectSerializer<>();
    }

    private class DiscardingElementWriter<IN> implements StatefulSinkWriter<IN, Void>, ProcessingTimeService.ProcessingTimeCallback {
        private static final long serialVersionUID = 1L;
        private ProcessingTimeService processingTimeService;
        private DiscardingElementWriter(ProcessingTimeService processingTimeService) {
            this.processingTimeService = processingTimeService;
        }

        public void write(IN element, SinkWriter.Context context) throws IOException, InterruptedException {
            logger.info("DiscardingElementWriter write - {}, watermark: {}, context: {}", element, context.currentWatermark(), context.timestamp());
        }

        public void flush(boolean endOfInput) throws IOException, InterruptedException {
        }

        @Override
        public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
            logger.info("DiscardingElementWriter writeWatermark : {}", watermark.getTimestamp());
        }

        public void close() throws Exception {
        }

        @Override
        public void onProcessingTime(long l) throws IOException, InterruptedException, Exception {
//            System.out.println("**** DiscardingElementWriter onProcessingTime ****");
            logger.info("**** DiscardingSink onProcessingTime **** {}", l);
//            registerNextBucketInspectionTimer();
        }

        @Override
        public List<Void> snapshotState(long l) throws IOException {
            return List.of();
        }

        private void initializeState() throws IOException {
//            registerNextBucketInspectionTimer();
        }

        private void registerNextBucketInspectionTimer() {
//            System.out.println("**** DiscardingElementWriter registerNextBucketInspectionTimer ****");
            final long nextInspectionTime =
                    processingTimeService.getCurrentProcessingTime() + 10 * 1000L; // 5 secs
            processingTimeService.registerTimer(nextInspectionTime, this);
        }
    }
}
