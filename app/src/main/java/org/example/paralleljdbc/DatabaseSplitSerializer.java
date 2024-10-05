package org.example.paralleljdbc;


import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Serializer for DatabaseSplit.
 */

public class DatabaseSplitSerializer implements SimpleVersionedSerializer<DatabaseSplit> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DatabaseSplit split) {
        return split.getTableName().getBytes();
    }

    @Override
    public DatabaseSplit deserialize(int version, byte[] serialized) {
        return new DatabaseSplit(new String(serialized));
    }
}
