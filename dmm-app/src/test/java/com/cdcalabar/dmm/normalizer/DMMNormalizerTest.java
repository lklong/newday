package com.cdcalabar.dmm.normalizer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.Row;
import org.junit.Test;

/**
 * Created by Administrator on 2016/7/26 0026.
 */
public class DMMNormalizerTest {

    private static String path = "target\\classes\\data\\sample_libsvm_data.txt";

    /**
     * 测试归一化
     */
    @Test
    public void testNormalizerWithP(){
        JavaRDD<LabeledPoint> rdd = DMMNormalizer.normalizerWithP("normalizer test",Double.POSITIVE_INFINITY,path);
        rdd.saveAsTextFile("target/tmp/norm/inftynorm");
    }

    /**
     * 测试归一化
     */
    @Test
    public void testDoNormalizer(){
        JavaRDD<Row> rowRDD = DMMNormalizer.doNormalizer("norm test",Double.POSITIVE_INFINITY,path,"","");
        rowRDD.saveAsTextFile("target/tmp/norm2/test_1");

    }
}
