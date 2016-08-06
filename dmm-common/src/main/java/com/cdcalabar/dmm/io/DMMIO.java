package com.cdcalabar.dmm.io;

import com.cdcalabar.dmm.io.params.InputParams;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/12 0012.
 */
public interface DMMIO {
     JavaPairRDD<String,String[]> getRDD(InputParams params) throws IOException;
     void saveRDD(JavaRDD<String[]> rdd, InputParams params) throws IOException;
}
