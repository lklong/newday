package com.cdcalabar.demo.env;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class SparkENV {

    private static JavaSparkContext sc ;
    /**
     * 环境初始化
     * @param appName
     * @return
     */
    public static JavaSparkContext initSC(String appName, String master){
        if (StringUtils.isBlank(master)){
            master = "local";
        }
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        sc = new JavaSparkContext(conf);
        return sc;
    }


}
