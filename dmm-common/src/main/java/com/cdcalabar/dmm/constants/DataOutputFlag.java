package com.cdcalabar.dmm.constants;

/**
 * Created by Administrator on 2016/7/17 0017.
 */
public enum  DataOutputFlag {
    HDFS_HBASE(0),HDFS(1),HBASE(2),HIVE(3);
    private Integer flag;

    public Integer getFlag() {
        return flag;
    }

    private DataOutputFlag(Integer flag){
        this.flag=flag;
    }

}
