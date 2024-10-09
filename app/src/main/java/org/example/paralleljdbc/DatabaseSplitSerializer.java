package org.example.paralleljdbc;


import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

/**
 * Serializer for DatabaseSplit.
 */

public class DatabaseSplitSerializer implements SimpleVersionedSerializer<DatabaseSplit> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(DatabaseSplit split) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(split);
            oos.flush();
            return bos.toByteArray();
        }
//        return split.getTableName().getBytes();
    }

    @Override
    public DatabaseSplit deserialize(int version, byte[] serialized) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            try {
                return (DatabaseSplit) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
//        return new DatabaseSplit(new String(serialized));
    }
}
