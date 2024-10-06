package org.example.paralleljdbc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Custom database source that uses source splits to handle multiple tables. i.e. one table is once split.
 *
 * Instructions:
 * https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface
 */
public class DatabaseSource implements Source<Row, DatabaseSplit, DatabaseSplitEnumeratorState>, ResultTypeQueryable<Row> {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final List<String> tables;
    private final Configuration config;

    public DatabaseSource(String jdbcUrl, String username, String password, List<String> tables, Configuration config) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tables = tables;
        this.config = config;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<Row, DatabaseSplit> createReader(SourceReaderContext readerContext) throws Exception {
        FutureCompletingBlockingQueue<RecordsWithSplitIds<Row>> elementsQueue = new FutureCompletingBlockingQueue<>();
        return new DatabaseSourceReader(new DatabaseSplitReader(readerContext, config, jdbcUrl, username, password), new DatabaseSplitRecordEmitter(), config, readerContext);
    }

    @Override
    public SplitEnumerator<DatabaseSplit, DatabaseSplitEnumeratorState> createEnumerator(SplitEnumeratorContext<DatabaseSplit> enumeratorContext) {
        return new DatabaseSplitEnumerator(enumeratorContext, tables, tables);
    }

    @Override
    public SplitEnumerator<DatabaseSplit, DatabaseSplitEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<DatabaseSplit> enumeratorContext, DatabaseSplitEnumeratorState checkpoint) {
        return new DatabaseSplitEnumerator(enumeratorContext, checkpoint.getTables(), checkpoint.getRemainingTables());
    }

    @Override
    public SimpleVersionedSerializer<DatabaseSplit> getSplitSerializer() {
        return new DatabaseSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<DatabaseSplitEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new DatabaseSplitEnumeratorStateSerializer();
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return TypeInformation.of(Row.class);
    }
}
