package com.cdcalabar.dmm.io.params;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;

/**
 * Created by lkl on 16-8-3.
 */
public class HiveParams extends InputParams{
    private HiveParamInput input;
    private HiveParamOutput output;

    public HiveParamInput getInput() {
        return input;
    }

    public void setInput(HiveParamInput input) {
        this.input = input;
    }

    public HiveParamOutput getOutput() {
        return output;
    }

    public void setOutput(HiveParamOutput output) {
        this.output = output;
    }

    @Override
    public String toString() {
        return ReflectionToStringBuilder.toString(this);
    }
}
