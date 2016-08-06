package com.cdcalabar.dmm.classification.decisiontree;

import com.cdcalabar.dmm.classification.decisiontree.predict.DMMDecisionTreePredict;
import com.cdcalabar.dmm.classification.decisiontree.train.DMMDecisionTreeTrain;
import com.cdcalabar.dmm.exception.DMMAlgorithmException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import org.junit.Test;

/**
 * Created by Administrator on 2016/7/22 0022.
 */
public class DecisionTreeTest {


    static String job_name = "DecisionTreeTest";

    static String input = "target\\classes\\data\\student-mat.csv";
    static String test_input = "target\\classes\\data\\student-por.csv";
    static String modelPath = "target\\tmp\\myDecisionTreeClassificationModel";
    static String predictPath = "target\\tmp\\classificationModelpredictPath\\stutdent";

    Integer numClasses = 2;
    Integer maxDepth = 8;
    Integer maxBins = 32;

    /**
     * 训练模型，保存合适的模型
     */
    @Test
    public void testEvaluateModel() throws DMMAlgorithmException {
//        产生模型
        DecisionTreeModel model = DMMDecisionTreeTrain.trainModel(job_name,input, numClasses, maxDepth, maxBins);
//        模型评估
        double rate = DMMDecisionTreeTrain.evaluateModel(model, test_input);
        System.out.println(rate);
//        保存合适的模型
        if (rate > 0.87) {
            DMMDecisionTreeTrain.saveModel(model, modelPath);
            System.out.println("已保存模型");
        } else {
            throw new DMMAlgorithmException("模型评估正确率不达标！  " + rate);
        }
    }

    /**
     * 使用模型预测业务数据，保存预测结果
     */
    @Test
    public void testUseModelToPredict2() {
        JavaRDD<String> rdd = DMMDecisionTreePredict.useModelToPredict(job_name, modelPath, test_input);
        DMMDecisionTreePredict.savePredictResult(rdd, predictPath);
    }


}
