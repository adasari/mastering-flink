package org.example.customsink.bucket;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

public class CustomBucketAssigner implements BucketAssigner<String, String> {
    @Override
    public String getBucketId(String o, Context context) {
        // TODO: add timestamp.
        return o;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
//        return new SimpleVersionedSerializerAdapter<>(StringSerializer.INSTANCE);
        return null;
    }
}
