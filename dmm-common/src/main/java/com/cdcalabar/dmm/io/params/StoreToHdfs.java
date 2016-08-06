package com.cdcalabar.dmm.io.params;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class StoreToHdfs  {
    private String out_path;
    private  String delimeter;
    private Integer need_head=1;
    private Integer[] header;

    public String getOut_path() {
        return out_path;
    }

    public void setOut_path(String out_path) {
        this.out_path = out_path;
    }

    public String getDelimeter() {
        return delimeter;
    }

    public void setDelimeter(String delimeter) {
        this.delimeter = delimeter;
    }

    public Integer[] getHeader() {
        return header;
    }

    public void setHeader(Integer[] header) {
        this.header = header;
    }

    public Integer getNeed_head() {
        return need_head;
    }

    public void setNeed_head(Integer need_head) {
        this.need_head = need_head;
    }
}
