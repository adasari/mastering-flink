package org.example.paralleljdbc;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Enumerator to manage the list of table splits.
 */
public class DatabaseSplitEnumerator implements SplitEnumerator<DatabaseSplit, DatabaseSplitEnumeratorState> {
    private final SplitEnumeratorContext<DatabaseSplit> context;
    private final List<String> tables;
    private final Queue<String> remainingTables;

    public DatabaseSplitEnumerator(SplitEnumeratorContext<DatabaseSplit> context, List<String> tables, List<String> remainingTables) {
        this.context = context;
        this.tables = tables;
        this.remainingTables = new ArrayDeque<>(remainingTables);
    }

    @Override
    public void start() {
        System.out.println("starting");
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // Logic to handle split requests (if needed)
        String nextTable = this.remainingTables.poll();
        if (nextTable != null) {
            this.context.assignSplit(new DatabaseSplit(nextTable), subtaskId);
        } else {
            this.context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<DatabaseSplit> splits, int subtaskId) {
        this.remainingTables.addAll(splits.stream().map(DatabaseSplit::getTableName).collect(Collectors.toList()));
    }

    @Override
    public void addReader(int subTaskId) {

    }

    @Override
    public DatabaseSplitEnumeratorState snapshotState(long checkpointId) throws Exception {
        return new DatabaseSplitEnumeratorState(tables, new ArrayList<>(remainingTables));
    }

    @Override
    public void close() {
        // Cleanup resources if needed
    }
}
