package org.example.customsink;


import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

/**
 * Serializer for FileWriterState.
 */

public class FileWriterStateSerializer implements SimpleVersionedSerializer<FileWriterState> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(FileWriterState state) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(state);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public FileWriterState deserialize(int version, byte[] serialized) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            try {
                return (FileWriterState) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
