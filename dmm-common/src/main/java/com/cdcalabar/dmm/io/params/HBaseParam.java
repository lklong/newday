package com.cdcalabar.dmm.io.params;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

import java.util.Map;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class HBaseParam extends InputParams {

    private Map<String,String> hbase_env;

    private HBaseParamInput input;

    private HBaseParamOutput output;

    public Map<String, String> getHbase_env() {
        return hbase_env;
    }

    public void setHbase_env(Map<String, String> hbase_env) {
        this.hbase_env = hbase_env;
    }

    public HBaseParamInput getInput() {
        return input;
    }

    public void setInput(HBaseParamInput input) {
        this.input = input;
    }

    public HBaseParamOutput getOutput() {
        return output;
    }

    public void setOutput(HBaseParamOutput output) {
        this.output = output;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
