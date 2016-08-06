package com.cdcalabar.dmm.classification.decisiontree;


import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.context.DMMContext;
import com.cdcalabar.dmm.io.factory.DMMIOFactory;
import com.cdcalabar.dmm.io.params.InputParams;
import com.cdcalabar.dmm.io.params.JsonParamsParser;
import com.cdcalabar.dmm.io.DMMIO;
import com.cdcalabar.dmm.serializer.BaseSerializble;
import com.cdcalabar.dmm.utils.DMMStringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/13 0013.
 */
public class MainAppTest extends BaseSerializble{

    private static final Logger LOGGER = LoggerFactory.getLogger(MainAppTest.class);

    private static   String hbaseParam = "{\n" +
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
            "            \"out_path\":\"hdfs://test:9000/user/hadoop/data/output/stu_out_hdfs\",\n" +
            "\t\t\t\"delimeter\":\"\\t\",\n" +
            "            \"need_header\":\"1\",\n" +
            "\t\t\t\"header\":[]\n" +
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
            "\t\"algorithm\":[\"params1\",\"params2\",\"params3\",\"...\"] \n" +
            "}";

    private static String hdfsParam = "{\n" +
            "  \"spark_env\":{\n" +
            "    \"spark.app.name\":\"hdfs-test\"\n" +
            "  },\n" +
            "  \"input\":{\n" +
            "    \"flag\":1,\n" +
            "    \"src\":[\"hdfs://test:9000/user/hadoop/data/input/students.txt\"],\n" +
            "    \"parse_regex\":\"\\t\",\n" +
            "    \"fields\":{\n" +
            "      \"1\":\"int\",\n" +
            "      \"5\":\"double\",\n" +
            "      \"6\":\"double\"\n" +
            "    }\n" +
            "  },\n" +
            "  \"output\":{\n" +
            "    \"flag\":\"2\",\n" +
            "    \"store_to_hdfs\":{\n" +
            "      \"out_path\":\"hdfs://test:9000/user/hadoop/data/output/stu_0_hdfs\",\n" +
            "      \"delimeter\":\"\\t\",\n" +
            "      \"need_head\":\"1\",\n" +
            "      \"header\":{\n" +
            "        \"1\":\"name1\",\n" +
            "        \"2\":\"name2\",\n" +
            "        \"3\":\"name3\"\n" +
            "      }\n" +
            "    },\n" +
            "    \"store_to_hbase\":{\n" +
            "      \"out_table\":\"stu_out_tab\",\n" +
            "      \"row_key\":\"1\",\n" +
            "      \"colnum_famliy\":[\"baseinfo\"],\n" +
            "      \"colnum\":{\n" +
            "        \"baseinfo:name\":\"1\",\n" +
            "        \"baseinfo:heigth\":\"5\",\n" +
            "        \"baseinfo:weight\":\"6\"\n" +
            "      }\n" +
            "    }\n" +
            "  },\n" +
            "  \"algorithm\":[\"params1\",\"params2\",\"params3\",\"...\"]\n" +
            "}";

    String hiveParam = "{\n" +
            "\t\"spark_env\":{\n" +
            "\t\t\"spark.app.name\":\"hive-test\",\n" +
            "\t\t\"spark.master\":\"local\"\n" +
            "    },\n" +
            "\t\"input\":{\n" +
            "\t\t\"flag\":3,\n" +
            "\t\t\"sql\":\"select name,heigth,weight from students\"\n" +
            "\t},\n" +
            "\t\"output\":{\n" +
            "\t\t\"flag\":3,\n" +
            "\t\t\"store_to_hive\":{\n" +
            "\t\t\t\"createTable\":1,\n" +
            "\t\t\t\"sql\":\"CREATE TABLE IF NOT EXISTS test_studuent_hive3 (name string, height double,weight double)row format delimited fields terminated by '\\t' stored as textfile\",\n" +
            "\t\t\t\"tableName\":\"test_studuent_hive3\",\n" +
            "\t\t\t\"colnums\":[\"name:string:1\",\"height:double:1\",\"weight:double:1\"],\n" +
            "\t\t\t\"hdfs_field_delimit\":\"\\t\"\n" +
            "\t\t}\n" +
            "    },\n" +
            "\t\"algorithm\":[\"params1\",\"params2\",\"params3\",\"...\"] \n" +
            "}";

    String hiveParamLzo = "{\n" +
            "\t\"spark_env\":{\n" +
            "\t\t\"spark.app.name\":\"hive-test\",\n" +
            "\t\t\"spark.master\":\"local\"\n" +
            "\t},\n" +
            "\t\"input\":{\n" +
            "\t\t\"flag\":3,\n" +
            "\t\t\"sql\":\"select id,name,age from lzo_db.lzo_test\"\n" +
            "\t},\n" +
            "\t\"output\":{\n" +
            "\t\t\"flag\":3,\n" +
            "\t\t\"store_to_hive\":{\n" +
            "\t\t\t\"createTable\":1,\n" +
            "\t\t\t\"sql\":\"CREATE TABLE IF NOT EXISTS test_studuent_hive3 (name string, height double,weight double)row format delimited fields terminated by '\\t' stored as textfile\",\n" +
            "\t\t\t\"tableName\":\"test_studuent_hive3\",\n" +
            "\t\t\t\"colnums\":[\"name:string:1\",\"height:double:1\",\"weight:double:1\"],\n" +
            "\t\t\t\"hdfs_field_delimit\":\"\\t\"\n" +
            "\t\t}\n" +
            "\t},\n" +
            "\t\"algorithm\":[\"params1\",\"params2\",\"params3\",\"...\"]\n" +
            "}";
    @Test
    public void testIO() {
//        String jsonp = hdfsParam;
//        String jsonp = hbaseParam;
//        String jsonp = hiveParam;
        String jsonp = hiveParamLzo;
        try {
            Integer flag =  JsonParamsParser.getInputFlag(jsonp);
            InputParams params = JsonParamsParser.parse(flag,jsonp);
            DMMIO io =  DMMIOFactory.getInstance(flag);
            JavaPairRDD<String ,String[]> rdd = io.getRDD(params);
            System.out.println(rdd.count());
        } catch (IOException e) {
            e.printStackTrace( );
        }
    }


    @Test
    public  void testSave() throws IOException {

        String jsonp = hiveParam;
        Integer flag =  JsonParamsParser.getInputFlag(jsonp);
        InputParams params = JsonParamsParser.parse(flag,jsonp);
        DMMIO io =  DMMIOFactory.getInstance(flag);

        Map<String, String> _params = new HashMap<String, String>();
        _params.put(DMMConstants.JOB_NAME, "hive-test");

        String[][] arr = new String[2][3];
        arr[0][0] = "laojiu";
        arr[0][1] = "189";
        arr[0][2] = "79";

        arr[1][0] = "张老实";
        arr[1][1] = "170";
        arr[1][2] = "69";

        JavaRDD<String[]> rdd = DMMContext.initSC(_params).parallelize(Arrays.asList(arr));
        try {
            io.saveRDD(rdd, params);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
