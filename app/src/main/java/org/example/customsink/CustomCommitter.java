package org.example.customsink;

import org.apache.flink.api.connector.sink2.Committer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class CustomCommitter implements Committer<FileSinkCommittable> {

    public static final Logger logger = LoggerFactory.getLogger(CustomCommitter.class);

    private String id;
    public CustomCommitter(String id) {
        this.id = id;
    }

    @Override
    public void commit(Collection<CommitRequest<FileSinkCommittable>> collection) throws IOException, InterruptedException {
        logger.info("**** CustomCommitter[{}] commit {} ****", id, collection);
    }

    @Override
    public void close() throws Exception {

    }
}
