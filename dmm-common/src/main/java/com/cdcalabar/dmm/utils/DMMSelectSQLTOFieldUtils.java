package com.cdcalabar.dmm.utils;

import com.cdcalabar.dmm.serializer.BaseSerializble;

/**
 * Created by lkl on 16-8-2.
 * 将select语句中的字段抽取出来
 */
public class DMMSelectSQLTOFieldUtils extends BaseSerializble {

    /**
     * 解析简单的sql,不包含临时with格式的临时表,不包含子查询，不包含多个select from
     * @param selectSQL
     * @return
     */
    public static String[] getFieldsFromSelectSQL(String selectSQL){
        selectSQL = selectSQL.toLowerCase();
        selectSQL = selectSQL.substring(selectSQL.indexOf("select")+"select".length(),selectSQL.indexOf("from"));
        String[] fields = selectSQL.split(",");
        for(int i = 0;i<fields.length;i++){
            if(fields[i].contains("as")){
                fields[i] = fields[i].split("as")[1].trim();
            }
        }
        return fields;
    }

    public static void main(String[] args) {
        System.out.println(DMMStringUtils.contactArray(getFieldsFromSelectSQL("select tt,yy,cc as dd from tab"),","));
    }
}
