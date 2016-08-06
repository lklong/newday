package com.cdcalabar.dmm.io.params;

/**
 * Created by lkl on 16-8-3.
 */
public class HiveParamOutput {

    private int flag;
    private StoreToHive store_to_hive;


    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public StoreToHive getStore_to_hive() {
        return store_to_hive;
    }

    public void setStore_to_hive(StoreToHive store_to_hive) {
        this.store_to_hive = store_to_hive;
    }
}
