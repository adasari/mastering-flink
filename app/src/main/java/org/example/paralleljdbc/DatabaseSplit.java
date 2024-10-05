package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceSplit;


/**
 * Split representing a single table.
 */

public class DatabaseSplit implements SourceSplit {
    private final String tableName;

    public DatabaseSplit(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String splitId() {
        return tableName;
    }

    public String getTableName() {
        return tableName;
    }
}
