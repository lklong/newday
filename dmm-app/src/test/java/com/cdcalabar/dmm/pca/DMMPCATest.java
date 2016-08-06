package com.cdcalabar.dmm.pca;

import org.apache.spark.mllib.linalg.distributed.RowMatrix;
import org.junit.Test;
/**
 * Created by THINK PAD on 2016/7/27.
 */
public class DMMPCATest {
    private static String path = "src\\main\\resources\\data\\PCA1.csv";
    /**
     * 测试PCA
     */
    @Test
    public void testDoPCA(){
        RowMatrix rdd = DMMPCA.doPCA("PCA test",path,4);
        //rdd.saveAsTextFile("target/tmp/pca/inftynorm");
    }
}