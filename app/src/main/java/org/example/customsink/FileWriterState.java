package org.example.customsink;

import java.io.Serializable;

public class FileWriterState implements Serializable {

    private String table;
    private String filePath;
    private long microBatchId;

    public FileWriterState(String table, String filePath, long microBatchId) {
        this.table = table;
        this.filePath = filePath;
        this.microBatchId = microBatchId;
    }
}
