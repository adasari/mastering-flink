package org.example.paralleljdbc;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

public class TableSplitRecords implements RecordsWithSplitIds<Row> {
    private final List<Row> rows;
    public TableSplitRecords(List<Row> rows) {
        this.rows = rows;
    }

    @Nullable
    @Override
    public String nextSplit() {
        return "";
    }

    @Nullable
    @Override
    public Row nextRecordFromSplit() {
        return null;
    }

    @Override
    public Set<String> finishedSplits() {
        return Set.of();
    }
}

