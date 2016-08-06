package com.cdcalabar.dmm.normalizer;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.context.DMMContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.feature.Normalizer;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/25 0025.
 */
public class DMMNormalizer {


    public static  JavaRDD<LabeledPoint> normalizerWithP(String jobName,Double p,String input){
//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);

//        加载数据
        JavaRDD<LabeledPoint> dataRDD = MLUtils.loadLibSVMFile(jsc.sc(), input).toJavaRDD();

//        构建p阶范数
        Normalizer normalizer1 = new Normalizer(p);

//      归一化处理
        JavaRDD<LabeledPoint> normRDD =  dataRDD.map(new Function<LabeledPoint, LabeledPoint>() {
            @Override
            public LabeledPoint call(LabeledPoint v1) throws Exception {
                return new LabeledPoint(v1.label(),normalizer1.transform(v1.features()));
            }
        });
        return normRDD;
    }


    public static JavaRDD<Row> doNormalizer(String jobName,Double p,String input,String features,String normFeatures){

//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);
        SQLContext jsql = new SQLContext(jsc);

//       加载数据
        JavaRDD<LabeledPoint> data =  MLUtils.loadLibSVMFile(jsc.sc(), input).toJavaRDD();
        DataFrame dataFrame = jsql.createDataFrame(data, LabeledPoint.class);

//        归一化对象创建
        org.apache.spark.ml.feature.Normalizer normalizer = new org.apache.spark.ml.feature.Normalizer()
                .setInputCol(DMMConstants.FEATURES)
                .setOutputCol(DMMConstants.NORMFEATURES)
                .setP(p);

//        归一化操作
        DataFrame normData = null;
        if(p.equals(Double.POSITIVE_INFINITY)){
             normData =  normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
        }else {
            normData = normalizer.transform(dataFrame);
        }
        JavaRDD<Row> rowRDD = normData.javaRDD();

        rowRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row.get(2));
            }
        });
        return rowRDD;
    }

}
