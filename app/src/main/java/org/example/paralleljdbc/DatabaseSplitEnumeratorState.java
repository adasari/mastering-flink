package org.example.paralleljdbc;

import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import java.util.List;

public class DatabaseSplitEnumeratorState {

    private final @Nonnull List<String> tables;
    private final @Nonnull List<String> remainingTables;

    public DatabaseSplitEnumeratorState(
            @Nonnull List<String> tables,
            @Nonnull List<String> remainingTables) {
        this.tables = Preconditions.checkNotNull(tables);
        this.remainingTables = Preconditions.checkNotNull(remainingTables);
    }

    @Nonnull
    public List<String> getTables() {
        return tables;
    }

    @Nonnull
    public List<String> getRemainingTables() {
        return remainingTables;
    }
}
