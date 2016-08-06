package com.cdcalabar.dmm.dict.impl;

import com.cdcalabar.dmm.dict.ModelDict;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * Created by Administrator on 2016/7/21 0021.
 */
public class ModelDictHBaseImpl implements ModelDict {
    @Override
    public void saveDict(Map<String, Integer> dict, String modelPath) {

    }

    @Override
    public JavaRDD getDict(String modelPath) {
        return null;
    }
}
