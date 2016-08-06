package com.cdcalabar.dmm.io.params;

/**
 * Created by lkl on 16-8-3.
 */
public class StoreToHive {
    private int createTable;
    private String tableName;
    private String sql;
    private String[] colnums;
    private String hdfs_field_delimit;

    public String getHdfs_field_delimit() {
        return hdfs_field_delimit;
    }

    public void setHdfs_field_delimit(String hdfs_field_delimit) {
        this.hdfs_field_delimit = hdfs_field_delimit;
    }

    public String[] getColnums() {
        return colnums;
    }

    public void setColnums(String[] colnums) {
        this.colnums = colnums;
    }

    public int getCreateTable() {
        return createTable;
    }

    public void setCreateTable(int createTable) {
        this.createTable = createTable;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
