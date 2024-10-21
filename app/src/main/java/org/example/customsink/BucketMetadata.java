package org.example.customsink;

import java.io.BufferedWriter;
import java.io.Serializable;

public class BucketMetadata implements Serializable {

    private String tableName;
    private long microBatchId;
    private boolean isCompleted;
    private String filePath;
    private BufferedWriter writer;

    public BucketMetadata(String tableName, long microBatchId, boolean isCompleted, String filePath, BufferedWriter writer) {
        this.tableName = tableName;
        this.microBatchId = microBatchId;
        this.isCompleted = isCompleted;
        this.filePath = filePath;
        this.writer = writer;
    }

    public BucketMetadata(String tableName, long microBatchId, String filePath, BufferedWriter writer) {
        this.tableName = tableName;
        this.microBatchId = microBatchId;
        this.isCompleted = false;
        this.filePath = filePath;
        this.writer = writer;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getMicroBatchId() {
        return microBatchId;
    }

    public void setMicroBatchId(long microBatchId) {
        this.microBatchId = microBatchId;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }

    public BufferedWriter getWriter() {
        return writer;
    }

    public void setWriter(BufferedWriter writer) {
        this.writer = writer;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
