package com.cdcalabar.dmm.context;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/12 0012.
 * DMMContext 设置为单例
 */
public class DMMContext {

    public static JavaSparkContext sc ;

    private static Map<String, String> sparkParams;

    /**
     * 初始化参数
     */
    static {
        sparkParams = new HashMap<String, String>();
        sparkParams.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkParams.put("spark.master","local");
        sparkParams.put("spark.eventLog.enabled","true");
        sparkParams.put("spark.eventLog.dir","/home/lkl/spark-events");
        sparkParams.put("spark.sql.warehouse.dir","hdfs://test:9000/user/hive/warehouse");
        sparkParams.put("spark.executor.memory", "2g");
    }

    /**
     * 构建spark上下文
     *
     * @param _sparkParams
     * @return
     */
    public static JavaSparkContext initSC(Map<String,String> _sparkParams) {
        if(sc != null){
            return sc;
        }
        SparkConf sparkConf = new SparkConf();
        if (_sparkParams == null) {
            _sparkParams = sparkParams;
        }else {
            _sparkParams.putAll(sparkParams);
        }
        for (Map.Entry<String, String> entry : _sparkParams.entrySet()) {
            sparkConf.set(entry.getKey(), entry.getValue());
        }
        sc = new JavaSparkContext(sparkConf);
        return sc;
    }
}
