package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.types.Row;

// <Input type, OutputType, Split state>
public class DatabaseSplitRecordEmitter implements RecordEmitter<Row, Row, DatabaseSplitState> {
    @Override
    public void emitRecord(Row row, SourceOutput<Row> sourceOutput, DatabaseSplitState databaseSplit) throws Exception {
    }
}
