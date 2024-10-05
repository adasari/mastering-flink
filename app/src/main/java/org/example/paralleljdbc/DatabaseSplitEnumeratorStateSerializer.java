package org.example.paralleljdbc;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.Collections;

public class DatabaseSplitEnumeratorStateSerializer implements SimpleVersionedSerializer<DatabaseSplitEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(DatabaseSplitEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(state);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public DatabaseSplitEnumeratorState deserialize(int version, byte[] data)
            throws IOException {

        if (version != CURRENT_VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            try {
                return (DatabaseSplitEnumeratorState) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }
}