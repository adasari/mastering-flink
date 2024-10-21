package org.example.customsink;


import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;

/**
 * Serializer for object.
 */

public class GenericObjectSerializer<T> implements SimpleVersionedSerializer<T> {
    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(obj);
            oos.flush();
            return bos.toByteArray();
        }
    }

    @Override
    public T deserialize(int version, byte[] serialized) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            try {
                return (T) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
