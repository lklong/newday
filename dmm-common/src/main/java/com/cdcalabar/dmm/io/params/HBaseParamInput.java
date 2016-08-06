package com.cdcalabar.dmm.io.params;


/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class HBaseParamInput   {

    private Integer flag;
    private String[] table;
    private HBaseParamQuery query;

    public Integer getFlag() {
        return flag;
    }

    public void setFlag(Integer flag) {
        this.flag = flag;
    }

    public HBaseParamQuery getQuery() {
        return query;
    }

    public void setQuery(HBaseParamQuery query) {
        this.query = query;
    }

    public String[] getTable() {
        return table;
    }

    public void setTable(String[] table) {
        this.table = table;
    }
}

