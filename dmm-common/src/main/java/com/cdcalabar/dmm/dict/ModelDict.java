package com.cdcalabar.dmm.dict;

import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * Created by Administrator on 2016/7/21 0021.
 * 模型字典保存,读取
 */
public interface ModelDict {

    /** 模型的字典数据保存路径 */
    String DICT_SUFFIX_PATH = "dict";

    /**
     * 存储模型的字典数据
     * @param dict 字典数据
     * @param modelPath 模型路径
     */
    void saveDict(Map<String,Integer> dict, String modelPath);

    /**
     * 获取模型的字典数据
     * @param modelPath
     * @return
     */
    JavaRDD getDict(String modelPath);


}
