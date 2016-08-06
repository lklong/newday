package com.cdcalabar.dmm.io.params;


/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class HBaseParamOutput   {
    private Integer flag;
    private StoreToHdfs store_to_hdfs;
    private StoreToHbase store_to_hbase;

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public StoreToHbase getStore_to_hbase() {
        return store_to_hbase;
    }

    public void setStore_to_hbase(StoreToHbase store_to_hbase) {
        this.store_to_hbase = store_to_hbase;
    }

    public StoreToHdfs getStore_to_hdfs() {
        return store_to_hdfs;
    }

    public void setStore_to_hdfs(StoreToHdfs store_to_hdfs) {
        this.store_to_hdfs = store_to_hdfs;
    }
}
