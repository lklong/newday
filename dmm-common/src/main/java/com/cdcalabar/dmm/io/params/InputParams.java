package com.cdcalabar.dmm.io.params;

import java.util.Map;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class InputParams {

    private Map<String, String> spark_env;

    private String[] algorithm;

    public String[] getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String[] algorithm) {
        this.algorithm = algorithm;
    }

    public Map<String, String> getSpark_env() {
        return spark_env;
    }

    public void setSpark_env(Map<String, String> spark_env) {
        this.spark_env = spark_env;
    }


}
