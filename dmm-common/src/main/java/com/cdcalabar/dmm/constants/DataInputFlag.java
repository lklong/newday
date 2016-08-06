package com.cdcalabar.dmm.constants;

public enum  DataInputFlag{
    HDFS(1),HBASE(2),HIVE(3);
    private int flag;
    private DataInputFlag(Integer flag){
        this.flag=flag;
    }

    public int getFlag() {
        return flag;
    }

    public static void main(String[] args) {
        System.out.println( DataInputFlag.HDFS.getFlag());
    }

}
