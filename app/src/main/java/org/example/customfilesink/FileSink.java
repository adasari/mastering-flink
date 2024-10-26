/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.example.customfilesink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.SupportsConcurrentExecutionAttempts;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.connector.sink2.*;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.*;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.FlinkRuntimeException;
import org.example.customfilesink.committer.FileCommitter;
import org.example.customfilesink.writer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class FileSink<IN>
        implements Sink<IN>,
        SupportsWriterState<IN, FileWriterBucketState>,
        SupportsCommitter<FileSinkCommittable>,
                SupportsWriterState.WithCompatibleState,
                SupportsPreCommitTopology<FileSinkCommittable, FileSinkCommittable>,
                SupportsConcurrentExecutionAttempts {

    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);

    private final BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder;

    private FileSink(BucketsBuilder<IN, ? extends BucketsBuilder<IN, ?>> bucketsBuilder) {
        this.bucketsBuilder = checkNotNull(bucketsBuilder);
    }

    @Override
    public FileWriter<IN> createWriter(WriterInitContext context) throws IOException {
        logger.info("Creating FileWriter instance {}", context);
        return restoreWriter(context, Collections.emptyList());
    }

    @Override
    public FileWriter<IN> restoreWriter(
            WriterInitContext context, Collection<FileWriterBucketState> recoveredState)
            throws IOException {
        FileWriter<IN> writer = bucketsBuilder.createWriter(context);
        writer.initializeState(recoveredState);
        return writer;
    }

    @Override
    public SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer() {
        try {
            return bucketsBuilder.getWriterStateSerializer();
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create writer state serializer.", e);
        }
    }

    @Override
    public Committer<FileSinkCommittable> createCommitter(CommitterInitContext context)
            throws IOException {
        return bucketsBuilder.createCommitter();
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer() {
        try {
            return bucketsBuilder.getCommittableSerializer();
        } catch (IOException e) {
            // it's not optimal that we have to do this but creating the serializers for the
            // FileSink requires (among other things) a call to FileSystem.get() which declares
            // IOException.
            throw new FlinkRuntimeException("Could not create committable serializer.", e);
        }
    }

    @Override
    public SimpleVersionedSerializer<FileSinkCommittable> getWriteResultSerializer() {
        return getCommittableSerializer();
    }

    @Override
    public Collection<String> getCompatibleWriterStateNames() {
        // StreamingFileSink
        return Collections.singleton("bucket-states");
    }

    public static <IN> DefaultRowFormatBuilder<IN> forRowFormat(
            final Path basePath, final Encoder<IN> encoder) {
        return new DefaultRowFormatBuilder<>(basePath, encoder, new DateTimeBucketAssigner<>());
    }

    public static <IN> DefaultBulkFormatBuilder<IN> forBulkFormat(
            final Path basePath, final BulkWriter.Factory<IN> bulkWriterFactory) {
        return new DefaultBulkFormatBuilder<>(
                basePath, bulkWriterFactory, new DateTimeBucketAssigner<>());
    }

    @Override
    public DataStream<CommittableMessage<FileSinkCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<FileSinkCommittable>> committableStream) {
        return committableStream;
    }

    /** The base abstract class for the {@link RowFormatBuilder} and {@link BulkFormatBuilder}. */
    @Internal
    private abstract static class BucketsBuilder<IN, T extends BucketsBuilder<IN, T>>
            implements Serializable {

        private static final long serialVersionUID = 1L;

        protected static final long DEFAULT_BUCKET_CHECK_INTERVAL = 60L * 1000L;

        @SuppressWarnings("unchecked")
        protected T self() {
            return (T) this;
        }

        @Internal
        abstract FileWriter<IN> createWriter(final WriterInitContext context) throws IOException;

        @Internal
        abstract FileCommitter createCommitter() throws IOException;

        @Internal
        abstract SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
                throws IOException;

        @Internal
        abstract SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
                throws IOException;

        @Internal
        abstract boolean isCompactDisabledExplicitly();

        @Internal
        abstract BucketWriter<IN, String> createBucketWriter() throws IOException;
    }

    /** A builder for configuring the sink for row-wise encoding formats. */
    public static class RowFormatBuilder<IN, T extends RowFormatBuilder<IN, T>>
            extends BucketsBuilder<IN, T> {

        private static final long serialVersionUID = 1L;

        private final Path basePath;

        private long bucketCheckInterval;

        private final Encoder<IN> encoder;

        private final FileWriterBucketFactory<IN> bucketFactory;

        private BucketAssigner<IN, String> bucketAssigner;

        private RollingPolicy<IN, String> rollingPolicy;

        private OutputFileConfig outputFileConfig;

        private boolean isCompactDisabledExplicitly = false;

        protected RowFormatBuilder(
                Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
            this(
                    basePath,
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    encoder,
                    bucketAssigner,
                    DefaultRollingPolicy.builder().build(),
                    new DefaultFileWriterBucketFactory<>(),
                    OutputFileConfig.builder().build());
        }

        protected RowFormatBuilder(
                Path basePath,
                long bucketCheckInterval,
                Encoder<IN> encoder,
                BucketAssigner<IN, String> assigner,
                RollingPolicy<IN, String> policy,
                FileWriterBucketFactory<IN> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = checkNotNull(basePath);
            this.bucketCheckInterval = bucketCheckInterval;
            this.encoder = checkNotNull(encoder);
            this.bucketAssigner = checkNotNull(assigner);
            this.rollingPolicy = checkNotNull(policy);
            this.bucketFactory = checkNotNull(bucketFactory);
            this.outputFileConfig = checkNotNull(outputFileConfig);
        }

        public T withBucketCheckInterval(final long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(final BucketAssigner<IN, String> assigner) {
            this.bucketAssigner = checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(final RollingPolicy<IN, String> policy) {
            this.rollingPolicy = checkNotNull(policy);
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

        public T disableCompact() {
            this.isCompactDisabledExplicitly = true;
            return self();
        }

        /** Creates the actual sink. */
        public FileSink<IN> build() {
            return new FileSink<>(this);
        }

        @Override
        FileWriter<IN> createWriter(WriterInitContext context) throws IOException {
            OutputFileConfig writerFileConfig;
            writerFileConfig = outputFileConfig;

            return new FileWriter<>(
                    basePath,
                    context.metricGroup(),
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    writerFileConfig,
                    context.getProcessingTimeService(),
                    bucketCheckInterval);
        }

        @Override
        FileCommitter createCommitter() throws IOException {
            return new FileCommitter(createBucketWriter());
        }

        @Override
        boolean isCompactDisabledExplicitly() {
            return isCompactDisabledExplicitly;
        }

        @Override
        SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileWriterBucketStateSerializer(
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer());
        }

        @Override
        SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileSinkCommittableSerializer(
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
        }

        BucketWriter<IN, String> createBucketWriter() throws IOException {
            return new RowWiseBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(), encoder);
        }
    }

    /** Builder for the vanilla {@link FileSink} using a row format. */
    public static final class DefaultRowFormatBuilder<IN>
            extends RowFormatBuilder<IN, DefaultRowFormatBuilder<IN>> {
        private static final long serialVersionUID = -8503344257202146718L;

        private DefaultRowFormatBuilder(
                Path basePath, Encoder<IN> encoder, BucketAssigner<IN, String> bucketAssigner) {
            super(basePath, encoder, bucketAssigner);
        }
    }

    /** A builder for configuring the sink for bulk-encoding formats, e.g. Parquet/ORC. */
    @PublicEvolving
    public static class BulkFormatBuilder<IN, T extends BulkFormatBuilder<IN, T>>
            extends BucketsBuilder<IN, T> {

        private static final long serialVersionUID = 1L;

        private final Path basePath;

        private long bucketCheckInterval;

        private final BulkWriter.Factory<IN> writerFactory;

        private final FileWriterBucketFactory<IN> bucketFactory;

        private BucketAssigner<IN, String> bucketAssigner;

        private CheckpointRollingPolicy<IN, String> rollingPolicy;

        private Map<String, String> writerConfig = new HashMap<>();

        private static final String HDFS_NO_LOCAL_WRITE = "fs.hdfs.no-local-write";

        private OutputFileConfig outputFileConfig;

        private boolean isCompactDisabledExplicitly = false;

        protected BulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            this(
                    basePath,
                    DEFAULT_BUCKET_CHECK_INTERVAL,
                    writerFactory,
                    assigner,
                    OnCheckpointRollingPolicy.build(),
                    new DefaultFileWriterBucketFactory<>(),
                    OutputFileConfig.builder().build());
        }

        protected BulkFormatBuilder(
                Path basePath,
                long bucketCheckInterval,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner,
                CheckpointRollingPolicy<IN, String> policy,
                FileWriterBucketFactory<IN> bucketFactory,
                OutputFileConfig outputFileConfig) {
            this.basePath = checkNotNull(basePath);
            this.bucketCheckInterval = bucketCheckInterval;
            this.writerFactory = writerFactory;
            this.bucketAssigner = checkNotNull(assigner);
            this.rollingPolicy = checkNotNull(policy);
            this.bucketFactory = checkNotNull(bucketFactory);
            this.outputFileConfig = checkNotNull(outputFileConfig);
        }

        public T withBucketCheckInterval(final long interval) {
            this.bucketCheckInterval = interval;
            return self();
        }

        public T withBucketAssigner(BucketAssigner<IN, String> assigner) {
            this.bucketAssigner = checkNotNull(assigner);
            return self();
        }

        public T withRollingPolicy(CheckpointRollingPolicy<IN, String> rollingPolicy) {
            this.rollingPolicy = checkNotNull(rollingPolicy);
            return self();
        }

        public T disableLocalWriting() {
            this.writerConfig.put(HDFS_NO_LOCAL_WRITE, String.valueOf(true));
            return self();
        }

        public T withOutputFileConfig(final OutputFileConfig outputFileConfig) {
            this.outputFileConfig = outputFileConfig;
            return self();
        }

        public BulkFormatBuilder<IN, ? extends BulkFormatBuilder<IN, ?>> withNewBucketAssigner(
                BucketAssigner<IN, String> assigner) {
            checkState(
                    bucketFactory.getClass() == DefaultFileWriterBucketFactory.class,
                    "newBuilderWithBucketAssigner() cannot be called "
                            + "after specifying a customized bucket factory");
            return new BulkFormatBuilder<>(
                    basePath,
                    bucketCheckInterval,
                    writerFactory,
                    checkNotNull(assigner),
                    rollingPolicy,
                    bucketFactory,
                    outputFileConfig);
        }

        public T disableCompact() {
            this.isCompactDisabledExplicitly = true;
            return self();
        }

        /**
         * Creates the actual sink.
         */
        public Sink<IN> build() {
            return new FileSink<>(this);
        }

        @Override
        FileWriter<IN> createWriter(WriterInitContext context) throws IOException {
            OutputFileConfig writerFileConfig;
            /*
            if (compactStrategy == null) {
                writerFileConfig = outputFileConfig;
            } else {
                // Compaction is enabled. We always commit before compacting, so the file written by
                // writer should be hid.
                writerFileConfig =
                        OutputFileConfig.builder()
                                .withPartPrefix("." + outputFileConfig.getPartPrefix())
                                .withPartSuffix(outputFileConfig.getPartSuffix())
                                .build();
            }
             */
            writerFileConfig = outputFileConfig;
            return new FileWriter<>(
                    basePath,
                    context.metricGroup(),
                    bucketAssigner,
                    bucketFactory,
                    createBucketWriter(),
                    rollingPolicy,
                    writerFileConfig,
                    context.getProcessingTimeService(),
                    bucketCheckInterval);
        }

        @Override
        FileCommitter createCommitter() throws IOException {
            return new FileCommitter(createBucketWriter());
        }

        @Override
        boolean isCompactDisabledExplicitly() {
            return isCompactDisabledExplicitly;
        }

        @Override
        SimpleVersionedSerializer<FileWriterBucketState> getWriterStateSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileWriterBucketStateSerializer(
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer(),
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer());
        }

        @Override
        SimpleVersionedSerializer<FileSinkCommittable> getCommittableSerializer()
                throws IOException {
            BucketWriter<IN, String> bucketWriter = createBucketWriter();

            return new FileSinkCommittableSerializer(
                    bucketWriter.getProperties().getPendingFileRecoverableSerializer(),
                    bucketWriter.getProperties().getInProgressFileRecoverableSerializer());
        }

        BucketWriter<IN, String> createBucketWriter() throws IOException {
            return new BulkBucketWriter<>(
                    FileSystem.get(basePath.toUri()).createRecoverableWriter(writerConfig),
                    writerFactory);
        }
    }

    /**
     * Builder for the vanilla {@link FileSink} using a bulk format.
     *
     * @param <IN> record type
     */
    public static final class DefaultBulkFormatBuilder<IN>
            extends BulkFormatBuilder<IN, DefaultBulkFormatBuilder<IN>> {

        private static final long serialVersionUID = 7493169281036370228L;

        private DefaultBulkFormatBuilder(
                Path basePath,
                BulkWriter.Factory<IN> writerFactory,
                BucketAssigner<IN, String> assigner) {
            super(basePath, writerFactory, assigner);
        }
    }
}
