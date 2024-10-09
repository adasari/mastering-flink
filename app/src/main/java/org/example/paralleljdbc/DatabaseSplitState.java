package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceSplit;


/**
 * Split representing a single table.
 */

public class DatabaseSplitState extends DatabaseSplit {

    public DatabaseSplitState(DatabaseSplit split) {
        super(split.getTableName(), split.getHostName());
    }

    public DatabaseSplit toDatabaseSplit() {
        return new DatabaseSplit(getTableName(), getHostName());
    }

}
