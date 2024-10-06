package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;

// <Input type, OutputType, Split state>
public class DatabaseSplitRecordEmitter implements RecordEmitter<Record, Record, DatabaseSplitState> {
    @Override
    public void emitRecord(Record row, SourceOutput<Record> sourceOutput, DatabaseSplitState databaseSplit) throws Exception {
        sourceOutput.collect(row);
    }
}
