package com.cdcalabar.dmm.io.params;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class HBaseParamQuery  {
    private String start_row;
    private String stop_row;
    private String[] colnum_famliy;
    private String[] colnum;

    public String[] getColnum() {
        return colnum;
    }

    public void setColnum(String[] colnum) {
        this.colnum = colnum;
    }

    public String[] getColnum_famliy() {
        return colnum_famliy;
    }

    public void setColnum_famliy(String[] colnum_famliy) {
        this.colnum_famliy = colnum_famliy;
    }

    public String getStart_row() {
        return start_row;
    }

    public void setStart_row(String start_row) {
        this.start_row = start_row;
    }

    public String getStop_row() {
        return stop_row;
    }

    public void setStop_row(String stop_row) {
        this.stop_row = stop_row;
    }
}
