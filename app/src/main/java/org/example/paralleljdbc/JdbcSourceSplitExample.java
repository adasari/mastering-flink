package org.example.paralleljdbc;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.List;


public class JdbcSourceSplitExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        List<String> tables = List.of("products", "users");

        Configuration config = new Configuration();
        DatabaseSource dbSource = new DatabaseSource("jdbc:postgresql://localhost:5432/test",
                "postgres", "postgres", tables, config);

        DataStream<Row> dataStream = env.fromSource(dbSource, WatermarkStrategy.noWatermarks(), "DatabaseSource");
        dataStream.print();

        env.execute("Parallel table Jdbc example");
//        dataStream.executeAndCollect("Parallel Jdbc stream");
    }

//    public static class TableSourceSplitReader implements SplitReader<Row, DatabaseSplit> {
//
//        private final String jdbcUrl;
//        private final String user;
//        private final String password;
//        private Connection connection;
//        private ResultSet resultSet;
//        private String currentTable;
//        private boolean recordsFetched = false;
//
//        public TableSourceSplitReader(String jdbcUrl, String user, String password) {
//            this.jdbcUrl = jdbcUrl;
//            this.user = user;
//            this.password = password;
//        }
//
//        public void handleSplitsAssignment(List<DatabaseSplit> splits) {
//            if (splits.isEmpty()) {
//                return;
//            }
//            DatabaseSplit databaseSplit = splits.get(0);
//            currentTable = databaseSplit.getTableName();
//            try {
//                connection = DriverManager.getConnection(jdbcUrl, user, password);
//                resultSet = connection.createStatement().executeQuery("SELECT * FROM " + currentTable);
//            } catch (SQLException e) {
//                throw new RuntimeException("Error connecting to the database or executing query", e);
//            }
//        }
//
//        @Override
//        public RecordsWithSplitIds<Row> fetch() {
//            List<Row> records = new ArrayList<>();
//            if (resultSet != null) {
//                while (true) {
//                    try {
//                        if (!resultSet.next()) break;
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                    Row row = null;
//                    try {
//                        row = convertResultSetToRow(resultSet);
//                    } catch (SQLException e) {
//                        throw new RuntimeException(e);
//                    }
//                    records.add(row);
//                }
//            }
//            recordsFetched = true;
//            //return RecordsWithSplitIds.forRecords(currentTable, records.iterator());
//            return null;
//        }
//
//        @Override
//        public void handleSplitsChanges(SplitsChange<DatabaseSplit> splitsChange) {
//
//        }
//
//        private Row convertResultSetToRow(ResultSet resultSet) throws SQLException {
//            int columnCount = resultSet.getMetaData().getColumnCount();
//            Row row = new Row(columnCount);
//            for (int i = 1; i <= columnCount; i++) {
//                row.setField(i - 1, resultSet.getObject(i));
//            }
//            return row;
//        }
//
//        @Override
//        public void wakeUp() {
//            // Optional: Implement wake-up logic for paused/resumed sources if needed
//        }
//
//        @Override
//        public void close() throws Exception {
//            if (connection != null && !connection.isClosed()) {
//                connection.close();
//            }
//        }
//    }


    /**
     * Custom reader to read data from tables using the assigned splits.
     */
//
//    public static class TableSourceReaderOld extends SingleThreadFetcherManager<Row, TableSplit>
//            implements SourceReader<Row, TableSplit> {
//        private final String jdbcUrl;
//        private final String username;
//        private final String password;
//
//        public TableSourceReaderOld(String jdbcUrl, String username, String password) {
//            super(fetcherContext -> new DatabaseFetcher(fetcherContext, jdbcUrl, username, password));
//            this.jdbcUrl = jdbcUrl;
//            this.username = username;
//            this.password = password;
//        }
//
//        @Override
//        public void start() {
//            // Start the source reader
//        }
//
//        @Override
//        public InputStatus pollNext(ReaderOutput<Row> output) throws Exception {
//            return InputStatus.NOTHING_AVAILABLE;
//        }
//
//        @Override
//        public List<TableSplit> snapshotState(long checkpointId) throws Exception {
//            return getAssignedSplits();
//        }
//
//        @Override
//        public CompletableFuture<Void> isAvailable() {
//            return null;
//        }
//
//        @Override
//        public void addSplits(List<TableSplit> splits) {
//            splits.forEach(split -> getSplitFetcher().fetchSplit(split));
//        }
//
//        @Override
//        public void notifyNoMoreSplits() {
//            // Notification for no more splits
//        }
//
//        @Override
//        public void close() throws Exception {
//            // Close resources if needed
//        }
//    }
}
