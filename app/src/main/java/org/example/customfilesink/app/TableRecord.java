package org.example.customfilesink.app;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@Setter
@ToString
public class TableRecord {
    private long sequenceId;
    private String table;
    private String name;
}
