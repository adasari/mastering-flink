package org.example.paralleljdbc;

public class Record {
    private long id;
    private String tableName;
    private String hostName;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    @Override
    public String toString() {
        return "Record Anil [id = " + this.getId() + ", tableName = "+ this.getTableName() + ", hostName = "+ this.getHostName() + "]";
    }
}
