package com.cdcalabar.dmm.io;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.constants.DataOutputFlag;
import com.cdcalabar.dmm.context.DMMContext;
import com.cdcalabar.dmm.io.params.*;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/12 0012.
 */
public class SparkHDFS implements DMMIO {


    public static JavaPairRDD readFromHDFS(InputParams params) {
        HDFSParam hdfsParam = (HDFSParam) params;
        JavaSparkContext sc = DMMContext.initSC(params.getSpark_env());
        JavaRDD<String> rdd = sc.textFile(hdfsParam.getInput().getSrc()[0]);
        String parse_regex = hdfsParam.getInput().getParse_regex();

//        取表头
        String[] colnumNames = rdd.first().split(parse_regex);
        hdfsParam.getInput().setColnumNames(colnumNames);

        String[] fields = hdfsParam.getInput().getFields();
        Integer[] cols = new Integer[fields.length];

        for (int i = 0; i < cols.length; i++) {
            cols[i] = Integer.valueOf(fields[i].split(DMMConstants.COLNUM_DELIMETER_COLON)[0]);
        }
//        表头过滤
        rdd = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return !v1.startsWith(colnumNames[0]);
            }
        });
//        构建pairRDD
        JavaPairRDD pairRdd = rdd.mapToPair(new PairFunction<String, String, String[]>() {
            @Override
            public Tuple2<String, String[]> call(String s) throws Exception {
                String[] arr = s.split(parse_regex);
                String key = arr[cols[0]];
                String[] value = new String[cols.length - 1];
                for (int i = 0; i < cols.length; i++) {
                    value[0] = arr[cols[i + 1]];
                }
                return new Tuple2<String, String[]>(key, value);
            }
        });
//      TODO 删除  测试代码
        pairRdd.foreach(new VoidFunction<Tuple2<String, String[]>>() {
            @Override
            public void call(Tuple2<String, String[]> stringTuple2) throws Exception {
                System.out.println("#######################");
                System.out.println(stringTuple2._1);
                System.out.println(stringTuple2._2);
            }
        });
        return pairRdd;
    }

    @Override
    public JavaPairRDD getRDD(InputParams params) {
        return readFromHDFS(params);
    }

    @Override
    public void saveRDD(JavaRDD<String[]> rdd, InputParams params) throws IOException {
        HDFSParam _params = (HDFSParam) params;
        HDFSParamInput input = _params.getInput();
        HDFSParamOutput output = _params.getOutput();
        Integer outputFlag = output.getFlag();
//      store to hdfs
        if (outputFlag.equals( DataOutputFlag.HDFS.getFlag())) {
            StoreToHdfs storeToHdfs = output.getStore_to_hdfs();
            String delimeter = storeToHdfs.getDelimeter();
            String out_path = storeToHdfs.getOut_path();
            StringBuffer buf = new StringBuffer();
            Integer need_header = storeToHdfs.getNeed_head();

            //  添加新文件表头
            String[] colnumNames = input.getColnumNames();
            Integer[] colnumNumbers = storeToHdfs.getHeader();
            if (need_header == 1) {
                for (Integer h : colnumNumbers) {
                    buf.append(colnumNames[h]).append(delimeter);
                }
                buf.append(DMMConstants.LINE_DELIMETER);
            }

            rdd.map(new Function<String[], String>() {
                @Override
                public String call(String[] v1) throws Exception {
                    for (String str : v1) {
                        buf.append(str).append(delimeter);
                    }
                    buf.append(DMMConstants.LINE_DELIMETER);
                    return buf.toString();
                }
            }).saveAsTextFile(out_path);

//      store to hbase
        } else if (outputFlag.equals( DataOutputFlag.HBASE.getFlag()) ){
            StoreToHbase storeToHbase = output.getStore_to_hbase();
            String tableName = storeToHbase.getOut_table();
            HTable table = SparkHBase.buildHTable(tableName);
            Map<String,String> cfs = storeToHbase.getColnum();
//            存放源文件的数据
            DMMContext.sc.textFile(input.getSrc()[0]).foreach(new VoidFunction<String>() {
                @Override
                public void call(String s) throws Exception {
                    String[] arr = s.split(DMMConstants.COLNUM_DELIMETER_COLON);
                    Put put = new Put(Bytes.toBytes(arr[0]));

                    for (Map.Entry<String,String> entry : cfs.entrySet()) {
                        String[] key_cf = entry.getKey().split(DMMConstants.COLNUM_DELIMETER_COLON);
                        put.add(Bytes.toBytes(key_cf[0]), Bytes.toBytes(key_cf[1]), Bytes.toBytes(arr[Integer.valueOf(entry.getValue())]));
                    }

                    table.put(put);
                    table.flushCommits();
                }
            });

//          存放计算结果数据
            rdd.foreach(new VoidFunction<String[]>() {
                @Override
                public void call(String[] values) throws Exception {
                    Put put = new Put(Bytes.toBytes(values[0]));
                    put.add(Bytes.toBytes(DMMConstants.RESULT), Bytes.toBytes(DMMConstants.RESULT), Bytes.toBytes(values[1]));
                    table.put(put);
                    table.flushCommits();
                }
            });


        }

    }

}
