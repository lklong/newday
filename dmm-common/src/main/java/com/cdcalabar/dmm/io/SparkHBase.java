package com.cdcalabar.dmm.io;

import com.cdcalabar.dmm.constants.DataOutputFlag;
import com.cdcalabar.dmm.context.DMMContext;
import com.cdcalabar.dmm.io.params.*;
import com.cdcalabar.dmm.serializer.BaseSerializble;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparkHBase extends BaseSerializble implements DMMIO {

    private static final Logger LOGGER = LoggerFactory.getLogger(SparkHBase.class);

    private static Map<String, String> hbaseParams;

    private static Configuration conf;

    private static final String SPLIT_FLAG = ":";

    private static final String NAME_JOIN_FLAG = "_";

    private static final String CHANG_LINE_REGEX = "\n";

    /**
     * 初始化参数
     */
    static {
        hbaseParams = new HashMap<String, String>();
        hbaseParams.put("hbase.zookeeper.property.clientPort", "2181");
        hbaseParams.put("hbase.zookeeper.quorum", "test");
        hbaseParams.put("hbase.master", "test:7077");
    }

    /**
     * 构建htable
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static HTable buildHTable(String tableName) throws IOException {
        conf = HBaseConfiguration.create();
        for (Map.Entry<String, String> entry : hbaseParams.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        return new HTable(conf, tableName);
    }

    /**
     * 构建hbaseConf
     *
     * @return
     * @throws IOException
     */
    public static Configuration buildHBaseConf(HBaseParam params) throws IOException {
        Map<String, String> _hbaseParams = params.getHbase_env();
        conf = HBaseConfiguration.create();
        if (_hbaseParams == null) {
            _hbaseParams = hbaseParams;
        } else {
            _hbaseParams.putAll(hbaseParams);
        }
        for (Map.Entry<String, String> entry : _hbaseParams.entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        HBaseParamQuery query = params.getInput().getQuery();
        conf.set(TableInputFormat.INPUT_TABLE, params.getInput().getTable()[0]);
        conf.set(TableInputFormat.SCAN_ROW_START, query.getStart_row());
        conf.set(TableInputFormat.SCAN_ROW_STOP, query.getStop_row());
        String[] cfs = query.getColnum_famliy();
        for (String cf : cfs) {
            conf.set(TableInputFormat.SCAN_COLUMN_FAMILY, cf);
        }
        String[] cols = query.getColnum();
        for (String col : cols) {
            conf.set(TableInputFormat.SCAN_COLUMNS, col);
        }
        Scan scan = new Scan();

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        String scanStr = Base64.encodeBytes(proto.toByteArray());
        conf.set(TableInputFormat.SCAN, scanStr);

        return conf;
    }

    /**
     * 读取数据
     *
     * @param params
     * @return
     * @throws IOException
     */
    public static JavaPairRDD<String,String[]> readFromHBase( InputParams params) throws IOException {
        HBaseParam hbaseParam = (HBaseParam) params;
        JavaSparkContext sc = DMMContext.initSC(params.getSpark_env());

        String[] colnum = hbaseParam.getInput().getQuery().getColnum();
        List<String[]> cols = new ArrayList<String[]>();
        for (String str : colnum) {
            cols.add(new String[]{str.substring(0, str.indexOf(":")), str.substring(str.indexOf(":")+1)});
        }

        Configuration conf = buildHBaseConf(hbaseParam);
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(conf, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);

        JavaPairRDD<String, String[]> pairRdd = hBaseRDD.mapToPair(new PairFunction<Tuple2<ImmutableBytesWritable, Result>, String, String[]>() {
            @Override
            public Tuple2<String, String[]> call(Tuple2<ImmutableBytesWritable, Result> resultTuple2) throws Exception {
                Result rowResult = resultTuple2._2();
                String rowKey = Bytes.toString(rowResult.getRow());
                String[] arr = new String[cols.size()];
                for (int i = 0; i < cols.size(); i++) {
                    arr[i] = Bytes.toString(rowResult.getValue(Bytes.toBytes(cols.get(i)[0]), Bytes.toBytes(cols.get(i)[1])));
                }
                return new Tuple2<String, String[]>(rowKey, arr);
            }
        });
        hBaseRDD.count();
        pairRdd.count();
        return pairRdd;

    }


    /**
     * 删除
     *
     * @param table
     * @throws IOException
     */
    public static void delete(HTable table) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("row1"));
        table.delete(delete);
    }

    @Override
    public JavaPairRDD<String,String[]> getRDD(InputParams params){
        try {
            return readFromHBase(params);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void saveRDD(JavaRDD<String[]> rdd, InputParams params) throws IOException {
        HBaseParam _params = (HBaseParam) params;
        HBaseParamInput input = _params.getInput();
        HBaseParamOutput output = _params.getOutput();
        Integer outputFlag = output.getFlag();
//      store to hdfs
        if (outputFlag.equals(DataOutputFlag.HDFS.getFlag())) {
            StoreToHdfs storeToHdfs = output.getStore_to_hdfs();
            String delimeter = storeToHdfs.getDelimeter();
            String out_path = storeToHdfs.getOut_path();
            StringBuffer buf = new StringBuffer();
            Integer need_header = storeToHdfs.getNeed_head();

            //  添加新文件表头
            String[] colnumNames = input.getQuery().getColnum();
            if (need_header == 1) {
                for (String h : colnumNames) {
                    buf.append(h.replace(SPLIT_FLAG, NAME_JOIN_FLAG)).append(delimeter);
                }
                buf.append(CHANG_LINE_REGEX);
            }

            rdd.map(new Function<String[], String>() {
                @Override
                public String call(String[] v1) throws Exception {
                    for (String str : v1) {
                        buf.append(str).append(delimeter);
                    }
                    buf.append(CHANG_LINE_REGEX);
                    return buf.toString();
                }
            }).saveAsTextFile(out_path);

//      store to hbase
        } else if (outputFlag.equals(DataOutputFlag.HBASE.getFlag())) {
            StoreToHbase storeToHbase = output.getStore_to_hbase();
            String[] cfs = input.getQuery().getColnum_famliy();
            String tableName = _params.getOutput().getStore_to_hbase().getOut_table();
            HTable table = buildHTable(tableName);

            rdd.foreach(new VoidFunction<String[]>() {
                @Override
                public void call(String[] values) throws Exception {
                    Put put = new Put(Bytes.toBytes(values[0]));
                    for (int i = 0; i < cfs.length; i++) {
                        String[] arr = cfs[1].split(SPLIT_FLAG);
                        put.add(Bytes.toBytes(arr[0]), Bytes.toBytes(arr[1]), Bytes.toBytes(values[i+1]));
                    }
                    table.put(put);
                    table.flushCommits();
                }
            });

        }
    }
}