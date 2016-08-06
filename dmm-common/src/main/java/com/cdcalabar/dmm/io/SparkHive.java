package com.cdcalabar.dmm.io;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.constants.DataOutputFlag;
import com.cdcalabar.dmm.context.DMMContext;
import com.cdcalabar.dmm.io.params.*;
import com.cdcalabar.dmm.utils.DMMHDFSUtils;
import com.cdcalabar.dmm.utils.DMMHiveUtils;
import com.cdcalabar.dmm.utils.DMMStringUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.xml.utils.StringBufferPool;
import org.dmg.pmml.TextIndex;
import scala.Tuple2;

import javax.xml.datatype.DatatypeConstants;
import java.io.IOException;
import java.util.*;

/**
 * Created by Administrator on 2016/7/21 0021.
 */
public class SparkHive implements DMMIO {


    @Override
    public JavaPairRDD getRDD(InputParams params) {
//        return readFromHiveToRowRDD(params);
        return readFromHiveToStrRDD(params);
    }

    @Override
    public void saveRDD(JavaRDD<String[]> rdd, InputParams params) throws IOException {
//        saveRDDToHiveByRow(rdd, params);
        saveRDDToHDFSTOHive(rdd, params);
    }


    /**
     * 从hive中读取数据
     *
     * @param params 参数集
     * @return
     */
    public static JavaPairRDD readFromHiveToStrRDD(InputParams params) {
//       参数处理
        HiveParams _params = (HiveParams) params;
        HiveParamInput input = _params.getInput();

//        初始化上下文
        JavaSparkContext jsc = DMMContext.initSC(params.getSpark_env());

//         初始化sqlContext
        HiveContext hiveCtx = new HiveContext(jsc.sc());

//        dml/ddl操作
        Accumulator<Double> acc = jsc.accumulator(0d);
        String num = acc.value().toString();
        JavaRDD<Row> rows = hiveCtx.sql(input.getSql()).toJavaRDD();
        JavaPairRDD hivePairRDD = rows.mapToPair(new PairFunction<Row, String, String[]>() {
            @Override
            public Tuple2<String, String[]> call(Row row) throws Exception {
                acc.add(1d);
                int colLen = row.length();
                StringBuffer rowBuf = new StringBuffer();
                for (int colNum = 0; colNum < colLen; colNum++) {
                    rowBuf.append(row.get(colNum));
                    if (colNum != colLen - 1) {
                        rowBuf.append(DMMConstants.COLNUM_DELIMETER_T);
                    }
                }
                return  new Tuple2<String, String[]>(num, rowBuf.toString().split(DMMConstants.COLNUM_DELIMETER_T));
            }
        });
//        Row[] results = hiveCtx.sql(input.getSql()).collect();
//        Accumulator<Integer> acc = jsc.accumulator(0);
//        List<Tuple2<Integer, String>> rowsStr = new ArrayList<Tuple2<Integer, String>>();
//        StringBuffer rowBuf = null;
//        for (int rowNum = 0; rowNum < results.length; rowNum++) {
//            acc.add(1);
//            rowBuf = new StringBuffer();
//            Row row = results[rowNum];
//            int colLen = row.length();
//            for (int colNum = 0; colNum < colLen; colNum++) {
//                rowBuf.append(row.get(colNum));
//                if (colNum != colLen - 1) {
//                    rowBuf.append(DMMConstants.COLNUM_DELIMETER_T);
//                }
//            }
//            rowsStr.add(new Tuple2<Integer, String>(acc.value(), rowBuf.toString()));
//        }

//        JavaPairRDD hivePairRDD = jsc.parallelizePairs(rowsStr);

        return hivePairRDD;
    }

    /**
     * 从hive中读取数据
     *
     * @param params 参数集
     * @return 返回行对象
     */
    public static JavaPairRDD readFromHiveToRowRDD(InputParams params) {
//       参数处理
        HiveParams _params = (HiveParams) params;
        HiveParamInput input = _params.getInput();

//        初始化上下文
        JavaSparkContext jsc = DMMContext.initSC(params.getSpark_env());

//         初始化sqlContext
        HiveContext hiveCtx = new HiveContext(jsc.sc());

//        读取数据
        Accumulator<Integer> acc = jsc.accumulator(0);
        Integer val_acc = acc.value();
        JavaRDD<Row> rowRDD = hiveCtx.sql(input.getSql()).toJavaRDD();
        JavaPairRDD<Integer, Row> hivePairRDD = rowRDD.mapToPair(new PairFunction<Row, Integer, Row>() {
            @Override
            public Tuple2<Integer, Row> call(Row row) throws Exception {
                acc.add(1);
                return new Tuple2<Integer, Row>(val_acc, row);
            }
        });
        return hivePairRDD;
    }


    /**
     * 将数据按行存储到hive 缺点：效率
     *
     * @throws IOException
     */
    public static void saveRDDToHiveByLine(JavaRDD<String[]> rdd, InputParams params) throws IOException {

//        参数处理
        HiveParams _params = (HiveParams) params;
        HiveParamOutput output = _params.getOutput();
        StoreToHive store_to_hive = output.getStore_to_hive();

//        初始化上下文
        JavaSparkContext jsc = DMMContext.initSC(params.getSpark_env());
        HiveContext hivCtx = new HiveContext(jsc);

//        建表
        hivCtx.sql(store_to_hive.getSql());

//        存入数据
        StringBuffer sqlBuf = new StringBuffer("SELECT ");
        rdd.foreach(new VoidFunction<String[]>() {
            @Override
            public void call(String[] cols) throws Exception {
                int col_len = cols.length;
                for (int i = 0; i < col_len; i++) {
                    sqlBuf.append(cols[i]);
                    if (i != col_len - 1) {
                        sqlBuf.append(DMMConstants.COLNUM_DELIMETER_COMMA);
                    }
                }
                hivCtx.sql("INSERT INTO TABLE " + store_to_hive.getTableName() + " SELECT * FROM (" + sqlBuf.toString() + ") t");
            }
        });
    }

    /**
     * rdd转DataFrame存入Hive 缺点：无法支持复杂类型的存入,不需要建表的sql
     *
     * @param rdd
     * @param params
     */
    public static void saveRDDToHiveByRow(JavaRDD<String[]> rdd, InputParams params) {

//        参数处理
        HiveParams _params = (HiveParams) params;
        HiveParamOutput output = _params.getOutput();
        StoreToHive store_to_hive = output.getStore_to_hive();
        String[] colnames = store_to_hive.getColnums();

//      初始化上下文
        JavaSparkContext jsc = DMMContext.initSC(params.getSpark_env());
        HiveContext hivCtx = new HiveContext(jsc);

//        构建RowRDD
        JavaRDD<Row> javaRdd = rdd.map(new Function<String[], Row>() {
            public Row call(String[] line) throws Exception {
                return RowFactory.create(DMMHiveUtils.getObects(line,colnames));//Row工厂类创建row
            }
        });

//        构建schema
        StructType structType = DMMHiveUtils.buildStructType(colnames);
        DataFrame df = hivCtx.createDataFrame(javaRdd, structType);

//        存入表
        df.saveAsTable(store_to_hive.getTableName());

    }


    /**
     * rdd转DataFrame存入Hive 缺点：数据拷贝
     *
     * @param rdd
     * @param params
     */
    public static void saveRDDToHDFSTOHive(JavaRDD<String[]> rdd, InputParams params) {

//        参数处理
        HiveParams _params = (HiveParams) params;
        HiveParamOutput output = _params.getOutput();
        StoreToHive store_to_hive = output.getStore_to_hive();
        String[] colnames = store_to_hive.getColnums();

//      初始化上下文
        JavaSparkContext jsc = DMMContext.initSC(params.getSpark_env());
        HiveContext hivCtx = new HiveContext(jsc);

//        建表
        hivCtx.sql(store_to_hive.getSql());

        String delimit = store_to_hive.getHdfs_field_delimit();
        JavaRDD<String> hdfsRDD = rdd.map(new Function<String[], String>() {
            @Override
            public String call(String[] v1) throws Exception {
                return DMMStringUtils.contactArray(v1, delimit);
            }
        });

        String tmp_path = "/tmp/"+store_to_hive.getTableName();
        try {
            DMMHDFSUtils.deletePath(tmp_path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        hdfsRDD.saveAsTextFile(tmp_path);

//        从hdfs拷贝数据到hive
        hivCtx.sql("LOAD DATA INPATH '"+tmp_path+"' OVERWRITE INTO TABLE "+store_to_hive.getTableName());

        try {
            DMMHDFSUtils.deletePath(tmp_path);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
