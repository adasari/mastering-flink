package org.example.customsink;

import java.io.Serializable;

public class CustomSinkCommittable implements Serializable {

    private final String tableName;
    private final long microBatchId;

    public CustomSinkCommittable(String tableName, long microBatchId) {
        this.microBatchId = microBatchId;
        this.tableName = tableName;
    }
}
