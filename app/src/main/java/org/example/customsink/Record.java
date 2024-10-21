package org.example.customsink;

import java.io.Serializable;

public class Record implements Serializable {

    private String table;
    private long timestamp; // microBatch Id
    private long sequenceId;

    public Record(String value, long timestamp, long sequenceId) {
        this.table = value;
        this.timestamp = timestamp;
        this.sequenceId = sequenceId;
    }

    public long getTimestamp() {
        return timestamp;
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

    @Override
    public String toString() {
        return "Record{" +
                "value='" + table + '\'' +
                ", timestamp=" + timestamp +
                ", sequenceId=" + sequenceId +
                '}';
    }
}

