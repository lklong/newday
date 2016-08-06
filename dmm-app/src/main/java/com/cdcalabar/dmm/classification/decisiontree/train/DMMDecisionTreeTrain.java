package com.cdcalabar.dmm.classification.decisiontree.train;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.context.DMMContext;
import com.cdcalabar.dmm.utils.StringFeaturesToDoule;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

import static com.cdcalabar.dmm.utils.StringFeaturesToDoule.exchangeSrcData;

/**
 * Created by liukailong on 2016/7/20 0020.
 * 决策树模型训练
 */
public class DMMDecisionTreeTrain {

    /**
     * 模型训练
     *
     * @param trainDataPath 模型数据训练数据文件路径
     * @param numClasses    lable（目标）的类别个数
     * @param maxDepth      决策树的最大层数
     * @param maxBins       特征最大分类值
     * @return
     */
    public static DecisionTreeModel trainModel(String jobName, String trainDataPath, Integer numClasses,
                                               Integer maxDepth, Integer maxBins) {
        //        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);
//      训练数据
        JavaRDD<LabeledPoint> trainingData = StringFeaturesToDoule.exchangeSrcData(jsc, trainDataPath);
        DecisionTreeModel model = trainModel(trainingData, numClasses, maxDepth, maxBins);
        return model;
    }


    /**
     * 训练模型
     *
     * @param trainingData
     * @return
     */
    public static DecisionTreeModel trainModel(JavaRDD<LabeledPoint> trainingData, Integer numClasses,
                                               Integer maxDepth, Integer maxBins) {
//  Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        String impurity = "entropy";

// Train a DecisionTree model for classification.
        final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, impurity, maxDepth, maxBins);

        return model;
    }


    /**
     * 评估模型
     *
     * @param model    决策树模型
     * @param testPath 测试数据
     */
    public static double evaluateModel(DecisionTreeModel model, String testPath) {
//        获取化上下文
        JavaSparkContext jsc = DMMContext.sc;

        JavaRDD<LabeledPoint> testData = exchangeSrcData(jsc, testPath);

        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();
        return 1 - testErr;
    }

    /**
     * 保存模型
     *
     * @param model
     * @param modelSavePath
     */
    public static void saveModel(DecisionTreeModel model, String modelSavePath) {
        model.save(DMMContext.sc.sc(), modelSavePath);
    }

}
