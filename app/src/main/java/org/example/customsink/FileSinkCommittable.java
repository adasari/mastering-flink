package org.example.customsink;

import java.io.Serializable;

public class FileSinkCommittable implements Serializable {

    private String table;
    private String filePath;
    private long microBatchId;

    public FileSinkCommittable(String table, String filePath, long microBatchId) {
        this.table = table;
        this.filePath = filePath;
        this.microBatchId = microBatchId;
    }

    @Override
    public String toString() {
        return "FileSinkCommittable{" +
                "table='" + table + '\'' +
                ", filePath='" + filePath + '\'' +
                ", microBatchId='" + microBatchId + '\'' +
                '}';
    }
}
