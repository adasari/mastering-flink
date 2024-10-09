package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Objects;


/**
 * Split representing a single table.
 */

public class DatabaseSplit implements SourceSplit, Serializable {
    private final String tableName;
    private String hostName;
    public DatabaseSplit(String tableName) {
        this.tableName = tableName;
    }

    public DatabaseSplit(String tableName, String hostName) {
        this.tableName = tableName;
        this.hostName = hostName;
    }

    @Override
    public String splitId() {
        return tableName;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DatabaseSplit)) return false;
        DatabaseSplit that = (DatabaseSplit) o;
        return Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableName);
    }
}
