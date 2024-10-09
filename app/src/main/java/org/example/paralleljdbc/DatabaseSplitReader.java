package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayDeque;
import java.util.Queue;

public class DatabaseSplitReader implements SplitReader<Record, DatabaseSplit> {

    private final Configuration config;
    @Nullable
    private DatabaseSplit currentSplit;
    private final Queue<DatabaseSplit> splits;

    protected boolean hasNextRecordCurrentSplit;
    private final SourceReaderContext context;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement statement;
    private transient ResultSet resultSet;

    public DatabaseSplitReader(SourceReaderContext context, Configuration config, String jdbcUrl, String username, String password) {
        this.context = context;
        this.splits = new ArrayDeque<>();
        this.config = config;
        this.hasNextRecordCurrentSplit = false;
        this.currentSplit = null;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }


    @Override
    public RecordsWithSplitIds<Record> fetch() throws IOException {
        boolean couldFetch = checkSplitOrStartNext();
        if (!couldFetch) {
            return new RecordsBySplits.Builder<Record>().build();
        }

        if (!hasNextRecordCurrentSplit) {
            return finishSplit();
        }

        assert currentSplit != null;
        RecordsBySplits.Builder<Record> recordsBuilder =
                new RecordsBySplits.Builder<>();
//        int batch = this.splitReaderFetchBatchSize;
        int batch = 100;
        while (batch > 0 && hasNextRecordCurrentSplit) {
            try {
                Record record = new Record();
                // populate row from result set
                record.setTableName(currentSplit.splitId());
                record.setId(resultSet.getLong("id"));
                record.setHostName(currentSplit.getHostName() + "_" + Thread.currentThread().getName());
                recordsBuilder.add(currentSplit.splitId(), record);
                batch--;
                hasNextRecordCurrentSplit = resultSet.next();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (!hasNextRecordCurrentSplit) {
            recordsBuilder.addFinishedSplit(currentSplit.splitId());
            closeResultSetAndStatement();
        }

        return recordsBuilder.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<DatabaseSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        splits.addAll(splitsChanges.splits());
    }

    @Override
    public void wakeUp() {

    }

    @Override
    public void close() throws Exception {
        closeResultSetAndStatement();
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        connection = null;
        currentSplit = null;
    }

    private boolean checkSplitOrStartNext() {
        try {
            if (hasNextRecordCurrentSplit && resultSet != null) {
                return true;
            }

            final DatabaseSplit nextSplit = splits.poll();
            if (nextSplit != null) {
                currentSplit = nextSplit;
                openResultSetForSplit(currentSplit);
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void openResultSetForSplit(DatabaseSplit split)
            throws SQLException  {
        getOrEstablishConnection();
        closeResultSetIfNeeded();
        prepareStatement(split);
        resultSet = statement.executeQuery();
        hasNextRecordCurrentSplit = resultSet.next();
    }

    private void getOrEstablishConnection() throws SQLException {
        connection = DriverManager.getConnection(this.jdbcUrl, username, password);
    }


    private void closeResultSetAndStatement() {
        closeResultSetIfNeeded();
        closeStatementIfNeeded();
    }

    private void closeResultSetIfNeeded() {
        try {
            if (resultSet != null && !resultSet.isClosed()) {
                resultSet.close();
            }
            resultSet = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void closeStatementIfNeeded() {
        try {
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            statement = null;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void prepareStatement(DatabaseSplit split) throws SQLException {
        closeStatementIfNeeded();
        statement =
                connection.prepareStatement(
                        "select * from "+ split.getTableName(), ResultSet. TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(1000);
    }

    private RecordsWithSplitIds<Record> finishSplit() {
        closeResultSetAndStatement();

        RecordsBySplits.Builder<Record> builder = new RecordsBySplits.Builder<>();
        assert currentSplit != null;
        builder.addFinishedSplit(currentSplit.splitId());
        currentSplit = null;
        return builder.build();
    }
}
