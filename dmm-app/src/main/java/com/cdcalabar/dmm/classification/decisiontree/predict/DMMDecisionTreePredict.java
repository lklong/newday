package com.cdcalabar.dmm.classification.decisiontree.predict;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.context.DMMContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;

import java.util.HashMap;
import java.util.Map;

import static com.cdcalabar.dmm.utils.StringFeaturesToDoule.*;

/**
 * Created by Administrator on 2016/7/20 0020.
 * 模型用于生产数据预测分类
 */
public class DMMDecisionTreePredict {

    /**
     * 使用模型预测生产数据
     *
     * @param modelPath
     * @param dataPath
     */
    public static JavaRDD<String> useModelToPredict(String jobName,String modelPath, String dataPath) {
//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);

        DecisionTreeModel model = DecisionTreeModel.load(jsc.sc(), modelPath);
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
                        if(y[0] == 0){
                            buf_line.append(header+",res\n");
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
        return predictionRDD;
    }

    /**
     * 保存预测结果
     */
    public static void savePredictResult(JavaRDD rdd,String out) {
        rdd.saveAsTextFile(out);
    }
}
