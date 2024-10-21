package org.example.customsink;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class CustomStatefulSinkWriter implements StatefulSinkWriter<Record, FileWriterState>,
        CommittingSinkWriter<Record, FileSinkCommittable>,
        ProcessingTimeService.ProcessingTimeCallback {
    public static final Logger logger = LoggerFactory.getLogger(CustomStatefulSinkWriter.class);

    int id;
    private Path basePath;
    private ProcessingTimeService processingTimeService;
    private Map<String, LinkedHashMap<Long, BucketMetadata>> tableToFileMap;

    /*
        private BucketAssigner<String, String> bucketAssigner;
        private BucketWriter<String, String> bucketWriter;

        public CustomStatefulSinkWriter(int taskNumber, Path basePath, BucketAssigner<String, String> bucketAssigner, BucketWriter<String, String> bucketWriter,
                                        OutputFileConfig outputFileConfig) {
            this.basePath = basePath;
            this.bucketAssigner = bucketAssigner;
            this.bucketWriter = bucketWriter;
            this.id = taskNumber;
        }
     */

    public CustomStatefulSinkWriter(int taskNumber, String basePath, ProcessingTimeService processingTimeService) {
        logger.debug("CustomStatefulSinkWriter constructor - {}", taskNumber);
        this.id = taskNumber;
        this.basePath = new Path(basePath);
        this.tableToFileMap = new HashMap<>();

        this.processingTimeService = processingTimeService;

    }

    @Override
    public List<FileWriterState> snapshotState(long checkpointId) throws IOException {
        logger.info("CustomStatefulSinkWriter[{}] snapshotState {}", id, checkpointId);
        return tableToFileMap.entrySet().stream()
                .flatMap(e -> e.getValue().entrySet().stream()
                        .map(microBatchEntry -> new FileWriterState(
                                    e.getKey(),
                                    microBatchEntry.getValue().getFilePath(),
                                    microBatchEntry.getKey()
                                )
                        )
                )
                .collect(Collectors.toList());
    }

    @Override
    public void write(Record record, Context context) throws IOException, InterruptedException {
        logger.info("CustomStatefulSinkWriter[{}] write {} at {}", id, record, processingTimeService.getCurrentProcessingTime());

        String table =  record.getTable();
        if (!tableToFileMap.containsKey(table)) {
            // create new table entry.
            String fileName = String.format("%d-%s.txt", id, UUID.randomUUID().toString());
            String filePath = basePath.getPath() + "/" + table + "/" + record.getTimestamp() + "/"+ fileName;

            java.nio.file.Path path = Paths.get(filePath);
            try {
                // Create the directory if it doesn't exist
                Files.createDirectories(path.getParent());
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                return;
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
            LinkedHashMap<Long, BucketMetadata> microBatchMap = new LinkedHashMap<>();
            microBatchMap.put(record.getTimestamp(), new BucketMetadata(table, record.getTimestamp(), filePath, writer));
            tableToFileMap.put(table, microBatchMap);
        } else if (!tableToFileMap.get(table).containsKey(record.getTimestamp())) {
            // create microBatch entry.

            String fileName = String.format("%d-%s.txt", id, UUID.randomUUID().toString());
            String filePath = basePath.getPath() + "/" + table + "/" + record.getTimestamp() + "/"+ fileName;

            java.nio.file.Path path = Paths.get(filePath);
            try {
                // Create the directory if it doesn't exist
                Files.createDirectories(path.getParent());
            } catch (IOException e) {
                System.err.println("Failed to create directory: " + e.getMessage());
                return;
            }

            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
            LinkedHashMap<Long, BucketMetadata> microBatchMap = tableToFileMap.get(table);
            // update all previous microbatches to completed.
            microBatchMap.forEach((microBatchId, bucketMetadata) -> {
                bucketMetadata.setCompleted(true);
                // TODO: flush and close writer.
            });
            // add new one microbatch entry.
            microBatchMap.put(record.getTimestamp(), new BucketMetadata(table, record.getTimestamp(), filePath, writer));
        }

        // add record to current microbatch buffered reader.
        BufferedWriter writer = tableToFileMap.get(table).get(record.getTimestamp()).getWriter();
        writer.write(table);
        writer.newLine();
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        logger.info("CustomStatefulSinkWriter[{}] flush", id);
        tableToFileMap.forEach((table, microBatchMap) -> {
            microBatchMap.forEach((microBatchId, batchMetadata) -> {
                try {
                    batchMetadata.getWriter().flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {
        logger.info("CustomStatefulSinkWriter[{}] watermark {}", id, watermark.getFormattedTimestamp());
    }

    @Override
    public void close() throws Exception {
        tableToFileMap.forEach((table, microBatchMap) -> {
            microBatchMap.forEach((microBatchId, batchMetadata) -> {
                try {
                    batchMetadata.getWriter().close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        });
    }

    @Override
    public Collection<FileSinkCommittable> prepareCommit() throws IOException, InterruptedException {
        // return completed microbatchees.
        List<FileSinkCommittable> committableList = new ArrayList<>();
        tableToFileMap.forEach((table, microBatchMap) -> {
            microBatchMap.forEach((microBatchId, batchMetadata) -> {
                if (batchMetadata.isCompleted()) {
                    committableList.add(new FileSinkCommittable(table, batchMetadata.getFilePath(), batchMetadata.getMicroBatchId()));
                }

                // TODO remove completed items from the map.
            });
        });

        return committableList;
    }

    @Override
    public void onProcessingTime(long time) throws IOException, InterruptedException, Exception {
        logger.info("CustomStatefulSinkWriter[{}] onProcessingTime - {}", id, time);
    }
}
