package com.cdcalabar.dmm.classification.svm.train;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.utils.StringFeaturesToDoule;
import com.cdcalabar.dmm.context.DMMContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by liukailong on 2016/7/26 0026.
 * 线性支持向量机分类
 */
public class DMMSVMWithSGDTrain {

    private static final Logger LOGGER = LoggerFactory.getLogger(DMMSVMWithSGDTrain.class);

    /**
     * 模型训练 向量格式数据
     *
     * @param jobName           作业名称
     * @param input             训练模型的历史数据路径
     * @param numIterations     迭代次数
     * @param stepSize          初始步长
     * @param regParam          正则化参数
     * @param miniBatchFraction 迭代时的分片大小
     * @return
     */
    public static SVMModel trainModel(String jobName, String input, int numIterations, double stepSize, double regParam, double miniBatchFraction) {
        LOGGER.debug("jobName:" + jobName);

//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);

//        加载训练数据
        JavaRDD<LabeledPoint> trainData = MLUtils.loadLibSVMFile(jsc.sc(), input).toJavaRDD();
        trainData.cache();

//        训练模型
        final SVMModel model = SVMWithSGD.train(trainData.rdd(), numIterations, stepSize, regParam, miniBatchFraction);

//        清除阈值
        model.clearThreshold();

        return model;
    }

    /**
     * 模型训练 通过文本格式数据
     *
     * @param jobName
     * @param input
     * @param numIterations
     * @param stepSize
     * @param regParam
     * @param miniBatchFraction
     * @return
     */
    public static SVMModel trainModelWithText(String jobName, String input, int numIterations, double stepSize, double regParam, double miniBatchFraction) {
//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);

//        加载训练数据
        JavaRDD<LabeledPoint> trainData = StringFeaturesToDoule.exchangeSrcData(jsc, input);
        trainData.cache();

//        训练模型
        final SVMModel model = SVMWithSGD.train(trainData.rdd(), numIterations, stepSize, regParam, miniBatchFraction);

//        清除阈值
        model.clearThreshold();

        return model;
    }


    /**
     * 模型评估
     *
     * @param model 模型
     * @param input 测试数据路径
     * @return
     */
    public static Double evaluateModelWithText(SVMModel model, String input) {
//        加载测试数据
        JavaRDD<LabeledPoint> testData = StringFeaturesToDoule.exchangeSrcData(DMMContext.sc, input);
        testData.cache();

        return evaluateModel(model, testData);
    }

    /**
     * 模型评估
     *
     * @param model
     * @param input
     * @return
     */
    public static Double evaluateModel(SVMModel model, String input) {
//        加载测试数据
        JavaRDD<LabeledPoint> testData = MLUtils.loadLibSVMFile(DMMContext.sc.sc(), input).toJavaRDD();
        testData.cache();

        return evaluateModel(model, testData);
    }

    /**
     * 模型评估
     *
     * @param model    模型
     * @param inputRDD 测试数据
     * @return
     */
    public static Double evaluateModel(SVMModel model, JavaRDD<LabeledPoint> inputRDD) {

//       预测结果
        JavaRDD<Tuple2<Object, Object>> scoreAndLabels = inputRDD.map(
                new Function<LabeledPoint, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        Double score = model.predict(p.features());
                        return new Tuple2<Object, Object>(score, p.label());
                    }
                });

//       根据预测结果评估模型的准确性,计算ROC,AUC
        BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(JavaRDD.toRDD(scoreAndLabels));
        double auc = metrics.areaUnderROC();

        return auc;
    }


}
