package com.cdcalabar.dmm.io.params;


import com.cdcalabar.dmm.constants.DataInputFlag;
import com.google.gson.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2016/7/14 0014.
 */
public class JsonParamsParser {

    private static  final String INPUT = "input";
    private static  final String FLAG = "flag";

    public static void main(String[] args) {
        String json = "{\n" +
                "\t\"spark_env\":{\n" +
                "\t\t\"spark.app.name\":\"hbase-test\",\n" +
                "\t\t\"spark.master\":\"local\"\n" +
                "    },\n" +
                "\t\"hbase_env\":{\n" +
                "\t\t\"hbase.zookeeper.property.clientPort\":\"2181\",\n" +
                "\t\t\"hbase.zookeeper.quorum\":\"test\",\n" +
                "\t\t\"hbase.master\":\"test:7077\"\n" +
                "    },\n" +
                "\t\"input\":{\n" +
                "\t\t\"flag\":2,\n" +
                "\t\t\"table\":[\"students\"],\n" +
                "\t\t\"query\":{\n" +
                "\t\t\t\"start_row\":\"1\",\n" +
                "\t\t\t\"stop_row\":\"6\",\n" +
                "\t\t\t\"colnum_famliy\":[\"baseinfo\"],\n" +
                "\t\t\t\"colnum\":[\"baseinfo:name\",\"baseinfo:heigth\",\"baseinfo:weight\"]\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"output\":{\n" +
                "\t\t\"flag\":\"2\",\n" +
                "\t\t\"store_to_hdfs\":{\n" +
                "            \"out_path\":\"stu_out_hdfs\",\n" +
                "\t\t\t\"delimeter\":\"\\t\",\n" +
                "            \"need_header\":\"1\",\n" +
                "\t\t\t\"header\":{\n" +
                "                \"0\":\"id\",\n" +
                "\t\t\t\t\"1\":\"name\",\n" +
                "\t\t\t\t\"2\":\"heigth\",\n" +
                "\t\t\t\t\"3\":\"weight\"\n" +
                "\t\t\t}\n" +
                "\t\t},\n" +
                "\t\t\"store_to_hbase\":{\n" +
                "            \"out_table\":\"stu_out_tab\",\n" +
                "\t\t\t\"row_key\":\"row_key\",\n" +
                "\t\t\t\"colnum_famliy\":[\"baseinfo\"],\n" +
                "\t\t\t\"colnum\":{\n" +
                "\t\t\t\t\"baseinfo:name\":\"name\",\n" +
                "\t\t\t\t\"baseinfo:heigth\":\"heigth\",\n" +
                "\t\t\t\t\"baseinfo:weight\":\"weight\"\n" +
                "\t\t\t}\n" +
                "\t\t}\n" +
                "    },\n" +
                "\t\"algorithm\":[\"params1\",\"params\",\"params3\",\"...\"] \n" +
                "}";

            System.out.println(getInputFlag(json));

    }

    public static InputParams parse(Integer flag,String json) throws IOException {
        Gson gson = new Gson();
        if (flag == DataInputFlag.HDFS.getFlag( )) {
            HDFSParam params = gson.fromJson(json, HDFSParam.class);
            return params;
        } else if (flag == DataInputFlag.HBASE.getFlag( )) {
            HBaseParam params = gson.fromJson(json, HBaseParam.class);
            return params;
        } else if(flag == DataInputFlag.HIVE.getFlag()){
            HiveParams params = gson.fromJson(json, HiveParams.class);
            return params;
        }
        return null;

    }


    /**
     * 获取JsonObject
     * @param json
     * @return
     */
    public static JsonObject parseJson(String json){
        JsonParser parser = new JsonParser();
        JsonObject jsonObj = parser.parse(json).getAsJsonObject();
        return jsonObj;
    }

    public static Integer getInputFlag(String json){
        JsonObject jsonObject = parseJson(json);
        Map<String,Object> params = toMap(jsonObject);
        Object obj = params.get(INPUT);
        Map<String,Object> map = (Map<String,Object>)obj;
        return Integer.valueOf(map.get(FLAG).toString());
    }

    public static Map<String, Object> toMap(JsonObject json) {
        Map<String, Object> map = new HashMap<String, Object>( );
        Set<Map.Entry<String, JsonElement>> entrySet = json.entrySet( );
        for (Iterator<Map.Entry<String, JsonElement>> iter = entrySet.iterator( ); iter.hasNext( ); ) {
            Map.Entry<String, JsonElement> entry = iter.next( );
            String key = entry.getKey( );
            Object value = entry.getValue( );
            if (value instanceof JsonArray)
                map.put((String) key, toList((JsonArray) value));
            else if (value instanceof JsonObject)
                map.put((String) key, toMap((JsonObject) value));
            else
                map.put((String) key, value);
        }
        return map;
    }

    /**
     * 将JSONArray对象转换成List集合
     *
     * @param json
     * @return
     */
    public static List<Object> toList(JsonArray json) {
        List<Object> list = new ArrayList<Object>( );
        for (int i = 0; i < json.size( ); i++) {
            Object value = json.get(i);
            if (value instanceof JsonArray) {
                list.add(toList((JsonArray) value));
            } else if (value instanceof JsonObject) {
                list.add(toMap((JsonObject) value));
            } else {
                list.add(value);
            }
        }
        return list;
    }

}
