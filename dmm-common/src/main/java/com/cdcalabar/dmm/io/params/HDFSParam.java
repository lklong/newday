package com.cdcalabar.dmm.io.params;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class HDFSParam extends InputParams {

    private HDFSParamInput input;

    private HDFSParamOutput output;

    public HDFSParamInput getInput() {
        return input;
    }

    public void setInput(HDFSParamInput input) {
        this.input = input;
    }

    public HDFSParamOutput getOutput() {
        return output;
    }

    public void setOutput(HDFSParamOutput output) {
        this.output = output;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }

}
