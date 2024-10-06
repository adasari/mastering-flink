package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;

import java.util.Map;

public class DatabaseSourceReader extends SingleThreadMultiplexSourceReaderBase<
        Record, // The record type produced by the source
        Record, // The intermediate record type
        DatabaseSplit, // The split type used by the reader
        DatabaseSplitState // split state
        > {

    public DatabaseSourceReader(SplitReader<Record, DatabaseSplit> splitReader, RecordEmitter<Record, Record, DatabaseSplitState> recordEmitter, Configuration config, SourceReaderContext context) {
        super(() -> splitReader, recordEmitter, config, context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, DatabaseSplitState> map) {
        context.sendSplitRequest();
    }

    @Override
    protected DatabaseSplitState initializedState(DatabaseSplit databaseSplit) {
        return new DatabaseSplitState(databaseSplit);
    }

    @Override
    protected DatabaseSplit toSplitType(String splitId, DatabaseSplitState state) {
        return state.toDatabaseSplit();
    }
}
