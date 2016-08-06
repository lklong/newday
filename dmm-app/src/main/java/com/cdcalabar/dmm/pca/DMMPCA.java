package com.cdcalabar.dmm.pca;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.context.DMMContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.HashMap;
import java.util.Map;
/**
 * Created by THINK PAD on 2016/7/28.
 */
public class DMMPCA {

    /**
     *主成分分析
     * @param jobName 作业名称
     * @param input 数据源
     * @param pcaNum 最终纬度
     * @return
     */
    public static RowMatrix doPCA(String jobName, String input, int pcaNum) {
//        初始化上下文
        Map<String, String> params = new HashMap<String, String>();
        params.put(DMMConstants.JOB_NAME, jobName);
        JavaSparkContext jsc = DMMContext.initSC(params);

//        初始化数据
        JavaRDD<String> rdd = jsc.textFile(input);

//        数据转换
        JavaRDD<Vector> vecRDD = rdd.map(new Function<String, Vector>() {
            @Override
            public Vector call(String v1) throws Exception {
                ;
                String[] arr = v1.split(DMMConstants.COLNUM_DELIMETER_COMMA);
                double[] _arr = new double[arr.length];
                for (int i = 0; i < arr.length; i++) {
                    _arr[i] = Double.valueOf(arr[i]);
                }
                return Vectors.dense(_arr);
            }
        });
//          rdd转行矩阵
        RowMatrix mat = new RowMatrix(vecRDD.rdd());

//          主成分计算
        Matrix pc = mat.computePrincipalComponents(pcaNum);
//          将点投影到低维空间
        RowMatrix projected = mat.multiply(pc);
        return projected;
    }
}
