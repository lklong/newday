package com.cdcalabar.dmm.io.proxy;

import com.cdcalabar.dmm.io.DMMIO;
import com.cdcalabar.dmm.io.params.HBaseParam;
import com.cdcalabar.dmm.io.params.InputParams;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;

/**
 * Created by Administrator on 2016/7/15 0015.
 */
public class HBaseProxy implements DMMIO {

    private DMMIO io;

    public HBaseProxy() {
    }

    public HBaseProxy(DMMIO io) {
        this.io = io;
    }

    public void doBefore() {

    }

    public void doAfter() {

    }

    @Override
    public JavaPairRDD getRDD(InputParams params) {
        doBefore();
        try {
            return io.getRDD(params);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void saveRDD(JavaRDD<String[]> rdd, InputParams params) throws IOException {
        io.saveRDD(rdd,(HBaseParam)params);
        doAfter();
    }
}
