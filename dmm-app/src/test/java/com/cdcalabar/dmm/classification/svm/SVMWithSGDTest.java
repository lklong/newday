package com.cdcalabar.dmm.classification.svm;

import com.cdcalabar.dmm.classification.svm.predict.DMMSVMWithSGDPredict;
import com.cdcalabar.dmm.classification.svm.train.DMMSVMWithSGDTrain;
import com.cdcalabar.dmm.context.DMMContext;
import org.apache.spark.mllib.classification.SVMModel;
import org.junit.Test;

/**
 * Created by Administrator on 2016/7/26 0026.
 * 支持向量机分类测试 模型训练，评估，模型保存，生产预测保存
 */
public class SVMWithSGDTest {
    private String job_name = "SVMWithSGD classifier test";
    private String input = "target\\classes\\data\\sample_libsvm_data.txt";
    private String output2 = "target\\tmp\\libsvm_output_test_2";
    private String output = "target\\tmp\\svm_output_test_1";
    private String modelPath2 = "target\\tmp\\svm_model_test_2";
    private String modelPath = "target\\tmp\\svm_model_test_1";
    static String src_scv_input = "target\\classes\\data\\student-mat.csv";
    static String test_input = "target\\classes\\data\\student-por.csv";
    static String test_output_predict = "target\\tmp\\svm_output_test_1";

    double threshold = 0;

    @Test
    public void testTrainModel() {
//        根据指定参数产生模型
        SVMModel model = DMMSVMWithSGDTrain.trainModel(job_name, input, 100, 1, 0.01, 1.0);

//        模型评估
        Double auc = DMMSVMWithSGDTrain.evaluateModel(model, input);

        System.out.println(auc);

//       模型存储
        if (auc > 0.7) {
            model.save(DMMContext.sc.sc(),modelPath2);
        }

    }

    /**
     * 测试文本行数据
     */
    @Test
    public void testTrainModelWithText(){

//        根据指定参数产生模型
        SVMModel model = DMMSVMWithSGDTrain.trainModelWithText(job_name, src_scv_input, 50, 1, 0.01, 1.0);

//        模型评估
        Double auc = DMMSVMWithSGDTrain.evaluateModelWithText(model, test_input);

        System.out.println(auc);

//       模型存储
        if (auc > 0.7) {
            model.save(DMMContext.sc.sc(),modelPath);
        }
    }

    /**
     * 生产预测保存预测结果
     */
    @Test
    public void testUseModelToPredictWithText(){
        DMMSVMWithSGDPredict.useModelToPredictWithText(job_name,modelPath,threshold,test_input,test_output_predict);
    }

    /**
     * 生产预测保存预测结果
     */
    @Test
    public void testUseModelToPredict(){
        DMMSVMWithSGDPredict.useModelToPredict(job_name,modelPath2,threshold,input,output2);
    }

}
