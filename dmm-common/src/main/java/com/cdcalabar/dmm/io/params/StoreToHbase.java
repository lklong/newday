package com.cdcalabar.dmm.io.params;

import java.util.Map;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class StoreToHbase   {
    private String out_table;
    private String row_key;
    private String[] colnum_famliy;
    private Map<String,String> colnum;

    public String getOut_table() {
        return out_table;
    }

    public void setOut_table(String out_table) {
        this.out_table = out_table;
    }

    public Map<String, String> getColnum() {
        return colnum;
    }

    public void setColnum(Map<String, String> colnum) {
        this.colnum = colnum;
    }

    public String[] getColnum_famliy() {
        return colnum_famliy;
    }

    public void setColnum_famliy(String[] colnum_famliy) {
        this.colnum_famliy = colnum_famliy;
    }

    public String getRow_key() {
        return row_key;
    }

    public void setRow_key(String row_key) {
        this.row_key = row_key;
    }
}
