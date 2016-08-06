package com.cdcalabar.dmm.classification.svm.predict;

import com.cdcalabar.dmm.constants.DMMConstants;

import com.cdcalabar.dmm.context.DMMContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;

import java.util.HashMap;
import java.util.Map;

import static com.cdcalabar.dmm.utils.StringFeaturesToDoule.*;

/**
 * Created by Administrator on 2016/7/26 0026.
 */
public class DMMSVMWithSGDPredict {

    /**
     * 生产预测，保存预测结果
     *
     * @param jobName   作业名称
     * @param modelPath 模型存储的路径
     * @param input     业务数据
     * @param output    预测结果
     */
    public static void useModelToPredict(String jobName, String modelPath, double threshold, String input, String output) {
//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);
//        加载生产数据
        JavaRDD<LabeledPoint> proData = MLUtils.loadLibSVMFile(jsc.sc(), input).toJavaRDD();
        proData.cache();

        useModelToPredictCommon(jobName, modelPath, threshold, proData, output);
    }


//    /**
//     * 生产预测，保存预测结果
//     * @param jobName
//     * @param modelPath
//     * @param input
//     * @param output
//     */
//    public static void useModelToPredictWithText(String jobName, String modelPath, String input, String output) {
////        初始化上下文
//        Map<String, String> params = new HashMap<String, String>();
//        params.put(DMMConstants.JOB_NAME, jobName);
//        JavaSparkContext jsc = DMMContext.initSC(params);
//
////        加载生产数据
//        JavaRDD<LabeledPoint> proData = StringFeaturesToDoule.exchangeSrcData(jsc,input);
//        proData.cache();
//        useModelToPredictCommon(jobName,modelPath,proData,output);
//
//    }

    /**
     * 生产预测，保存预测结果
     *
     * @param jobName   作业名称
     * @param modelPath 模型保存的路径
     * @param proData   业务数据
     * @param output    预测结果
     */
    public static void useModelToPredictCommon(String jobName, String modelPath, double threshold, JavaRDD<LabeledPoint> proData, String output) {

//        加载模型
        SVMModel model = SVMModel.load(DMMContext.sc.sc(), modelPath);

//        设置阈值
        model.setThreshold(threshold);

//       生产数据预测
        StringBuffer buffer = new StringBuffer();
        JavaRDD<String> scoreAndLabels = proData.map(
                new Function<LabeledPoint, String>() {
                    public String call(LabeledPoint p) {
                        Double score = model.predict(p.features());
                        System.out.println(score);
                        Double label = p.label();
                        Vector features = p.features();
                        buffer.append(score).append(DMMConstants.COLNUM_DELIMETER_T);
                        for (int i = 0; i < features.size(); i++) {
                            buffer.append(features.apply(i)).append(DMMConstants.COLNUM_DELIMETER_T);
                        }
                        buffer.append(DMMConstants.LINE_DELIMETER);
                        return buffer.toString();
                    }
                });
//        保存预测结果到文件
        scoreAndLabels.saveAsTextFile(output);
    }


    /**
     * 使用模型预测生产数据
     *
     * @param modelPath
     * @param dataPath
     */
    public static void useModelToPredictWithText(String jobName, String modelPath, double threshold, String dataPath, String output) {
//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);

//        加载模型
        SVMModel model = SVMModel.load(DMMContext.sc.sc(), modelPath);
        model.setThreshold(threshold);

        JavaRDD<LabeledPoint> proData = exchangeSrcData(jsc, dataPath);
        final int[] y = {0};
        JavaRDD<String> predictionRDD =
                proData.map(new Function<LabeledPoint, String>() {
                    @Override
                    public String call(LabeledPoint p) throws Exception {
                        Vector vecs = p.features();
                        Double pre = model.predict(vecs);
                        int fSize = p.features().size();
                        StringBuffer buf_line = new StringBuffer();
                        if (y[0] == 0) {
                            buf_line.append(header + ",res\n");
                            y[0] = 1;
                        }
                        for (int j = 0; j < fSize; j++) {
                            Double value = vecs.apply(j);
                            String fStrValue;
                            if (needChangeToNumCol.contains(j)) {
                                fStrValue = featuresMap2.get(value.intValue());
                            } else {
                                fStrValue = value.toString();
                            }
                            buf_line.append(fStrValue).append(",");
                        }
                        buf_line.append(p.label()).append(",");
                        buf_line.append(pre);
                        return buf_line.toString();
                    }
                });
        predictionRDD.saveAsTextFile(output);
    }
}
