package com.cdcalabar.dmm.io.proxy;

import com.cdcalabar.dmm.io.DMMIO;
import com.cdcalabar.dmm.io.params.InputParams;
import com.cdcalabar.dmm.serializer.BaseSerializble;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

/**
 * Created by lkl on 16-8-1.
 */
public class HiveProxy extends BaseSerializble implements DMMIO {

    private DMMIO io;

    public HiveProxy() {
    }

    public HiveProxy(DMMIO io){
        this.io = io;
    }
    @Override
    public JavaPairRDD getRDD(InputParams params) {
        try {
            return io.getRDD(params);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void saveRDD(JavaRDD<String[]> rdd, InputParams params) throws IOException {
        io.saveRDD(rdd,params);
    }

    public static void saveRDD2(JavaRDD<String[]> rdd, InputParams params){

    }
}
