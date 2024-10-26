package org.example.customfilesink.app;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import java.io.IOException;

public class TextFileBulkWriter implements BulkWriter<TableRecord> {
    private FSDataOutputStream stream;

    public TextFileBulkWriter(FSDataOutputStream stream) {
        this.stream = stream;
    }

    @Override
    public void addElement(TableRecord element) throws IOException {
        stream.write(element.toString().getBytes());
        stream.write(System.lineSeparator().getBytes());
    }

    @Override
    public void flush() throws IOException {
        stream.flush();
    }

    @Override
    public void finish() throws IOException {
        stream.flush();
    }
}
