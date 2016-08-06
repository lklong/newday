package com.cdcalabar.dmm.io.factory;

import com.cdcalabar.dmm.constants.DataInputFlag;
import com.cdcalabar.dmm.io.SparkHBase;
import com.cdcalabar.dmm.io.proxy.*;
import com.cdcalabar.dmm.io.DMMIO;
import com.cdcalabar.dmm.io.SparkHDFS;
import com.cdcalabar.dmm.io.SparkHive;

/**
 * Created by Administrator on 2016/7/13 0013.
 */
public class DMMIOFactory {

    public static DMMIO getInstance(Integer flag){
        if(flag == DataInputFlag.HBASE.getFlag()){
            return new HBaseProxy(new SparkHBase());
        }else if (flag == DataInputFlag.HDFS.getFlag()){
            return new HDFSProxy(new SparkHDFS());
        }else if(flag == DataInputFlag.HIVE.getFlag()){
            return new HiveProxy(new SparkHive());
        }else {
            return new HDFSProxy(new SparkHDFS());
        }
    }

}
