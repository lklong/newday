package com.cdcalabar.dmm.io.params;

/**
 * Created by Administrator on 2016/7/15 0015.
 */
public class HDFSParamInput {

    /**  该字段值在程序中获取，key:value => 列号:列名称 */
    private String[] colnumNames;
    private Integer flag;
    private String[] src;
    private String parse_regex;
    private String[] fields;

    public String[] getColnumNames() {
        return colnumNames;
    }

    public void setColnumNames(String[] colnumNames) {
        this.colnumNames = colnumNames;
    }

    public String[] getFields() {
        return fields;
    }

    public void setFields(String[] fields) {
        this.fields = fields;
    }

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public String getParse_regex() {
        return parse_regex;
    }

    public void setParse_regex(String parse_regex) {
        this.parse_regex = parse_regex;
    }

    public String[] getSrc() {
        return src;
    }

    public void setSrc(String[] src) {
        this.src = src;
    }
}
