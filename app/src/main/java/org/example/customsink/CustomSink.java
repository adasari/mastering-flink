package org.example.customsink;

import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class CustomSink implements Sink<Record>, SupportsCommitter<FileSinkCommittable>, SupportsWriterState<Record, FileWriterState>,
        SupportsWriterState.WithCompatibleState, SupportsPostCommitTopology<FileSinkCommittable>,
        SupportsPreCommitTopology<FileSinkCommittable, FileSinkCommittable> {

    public static final Logger logger = LoggerFactory.getLogger(CustomSink.class);

    private String basePath;

    public CustomSink(String basePath) {
        this.basePath = basePath;
    }


    @Override
    public SinkWriter<Record> createWriter(WriterInitContext context) throws IOException {
        logger.info("CustomSink createWriter[{}]", context.getTaskInfo().getIndexOfThisSubtask());

        return new CustomStatefulSinkWriter(
                context.getTaskInfo().getIndexOfThisSubtask(),
                this.basePath,
                context.getProcessingTimeService()
        );
    }

    @Override
    public Committer<FileSinkCommittable> createCommitter(CommitterInitContext committerInitContext) throws IOException {
        logger.info("CustomSink createCommitter[{}]", committerInitContext.getTaskInfo().getIndexOfThisSubtask());
        return new CustomCommitter(String.valueOf(committerInitContext.getTaskInfo().getIndexOfThisSubtask()));
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        logger.info("CustomSink getCommittableSerializer");
        return new GenericObjectSerializer<>();
    }

    @Override
    public StatefulSinkWriter<Record, FileWriterState> restoreWriter(WriterInitContext writerInitContext, Collection<FileWriterState> collection) throws IOException {
        logger.info("CustomSink restoreWriter");
        return new CustomStatefulSinkWriter(
                writerInitContext.getTaskInfo().getIndexOfThisSubtask(),
                this.basePath,
                writerInitContext.getProcessingTimeService()
        );
    }

    @Override
    public SimpleVersionedSerializer<FileWriterState> getWriterStateSerializer() {
        logger.info("CustomSink getWriterStateSerializer");
        return new GenericObjectSerializer<>();
    }

    @Override
    public Collection<java.lang.String> getCompatibleWriterStateNames() {
        return List.of();
    }

    @Override
    public void addPostCommitTopology(DataStream<CommittableMessage<FileSinkCommittable>> dataStream) {
    }

    @Override
    public DataStream<CommittableMessage<FileSinkCommittable>> addPreCommitTopology(DataStream<CommittableMessage<FileSinkCommittable>> dataStream) {
        return dataStream;
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getWriteResultSerializer() {
        return new GenericObjectSerializer<>();
    }


}
