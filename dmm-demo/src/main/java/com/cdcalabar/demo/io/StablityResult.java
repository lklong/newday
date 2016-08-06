package com.cdcalabar.demo.io;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class StablityResult {
    /**
     * 构建输入RDD
     * @param input
     * @return
     */
    public static JavaRDD<String> initRDD(JavaSparkContext sc,String input) {
        JavaRDD<String> inputRDD = sc.textFile(input);
        return inputRDD;
    }

    /**
     * 输入到存储层
     * @param out
     */
    public static void output(String out,JavaRDD<String> outRDD){
        outRDD.saveAsTextFile(out);
    }
}
