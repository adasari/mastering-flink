package org.example.customsink.bucket;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;

import java.io.IOException;

public class CustomBucketWriter implements BucketWriter<String, String> {
    @Override
    public InProgressFileWriter<String, String> openNewInProgressFile(String s, Path path, long l) throws IOException {
        return new CustomPartFileWriter();
    }

    @Override
    public CompactingFileWriter openNewCompactingFile(CompactingFileWriter.Type type, String s, Path path, long creationTime) throws IOException {
        return null;
    }

    @Override
    public InProgressFileWriter<String, String> resumeInProgressFileFrom(String s, InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable, long l) throws IOException {
        return null;
    }

    @Override
    public WriterProperties getProperties() {
        return null;
    }

    @Override
    public PendingFile recoverPendingFile(InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable) throws IOException {
        return new CustomPendingFile();
    }

    @Override
    public boolean cleanupInProgressFileRecoverable(InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable) throws IOException {
        return false;
    }

    static class CustomPartFileWriter implements InProgressFileWriter<String, String> {

        @Override
        public void write(String s, long l) throws IOException {
            
        }

        @Override
        public InProgressFileRecoverable persist() throws IOException {
            return null;
        }

        @Override
        public PendingFileRecoverable closeForCommit() throws IOException {
            return null;
        }

        @Override
        public void dispose() {

        }

        @Override
        public String getBucketId() {
            return "";
        }

        @Override
        public long getCreationTime() {
            return 0;
        }

        @Override
        public long getSize() throws IOException {
            return 0;
        }

        @Override
        public long getLastUpdateTime() {
            return 0;
        }
    }

    static class CustomPendingFile implements PendingFile {

        @Override
        public void commit() throws IOException {

        }

        @Override
        public void commitAfterRecovery() throws IOException {

        }
    }
}
