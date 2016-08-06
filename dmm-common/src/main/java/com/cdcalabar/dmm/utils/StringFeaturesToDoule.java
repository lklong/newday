package com.cdcalabar.dmm.utils;

import com.cdcalabar.dmm.constants.DMMConstants;
import com.cdcalabar.dmm.exception.DMMAlgorithmException;
import com.cdcalabar.dmm.serializer.BaseSerializble;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/20 0020.
 */
public class StringFeaturesToDoule extends BaseSerializble {

    public final static Map<String, Integer> featuresMap = new HashMap<String, Integer>();
    public final static Map<Integer, String> featuresMap2 = new HashMap<Integer, String>();
    public static String header;
    public final static LinkedHashSet<Integer> needChangeToNumCol = new LinkedHashSet<Integer>();
    static String reg_num = "^(\\-|\\+)?\\d+(\\.\\d+)?$";

    /**
     * 数据转换
     *
     * @param sc
     * @param path               数据文件路径
     *  classificationRule 分类规则
     *                           classificationRule[1] 第一类
     *                           classificationRule[2] 第二类
     *                           classificationRule[n] 第n类
     *                           格式："[0-4]","[5-9]","[10-15]","[16-20]"
     * @return
     */
    public static JavaRDD<LabeledPoint> exchangeSrcData(JavaSparkContext sc, String path) {

//      加载数据
        JavaRDD<String> rdd = sc.textFile(path);

//      数据筛选
        final int[] y = new int[]{0};

        rdd = rdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {

                String[] arr = v1.split(DMMConstants.COLNUM_DELIMETER_COMMA);
                boolean temp = y[0] == 0;
                if (!temp) {
                    for (int i = 0; i < arr.length; i++) {
                        String el = arr[i].trim();
                        if (!el.matches(reg_num)) {
                            featuresMap.putIfAbsent(el, featuresMap.size());
                            featuresMap2.put(featuresMap.get(el), el);
                            if (y[0] == 1) {
                                needChangeToNumCol.add(i);
                            }
                        }
                    }
                } else {
//                    取表头
                    header = v1;
                }
                y[0]++;
                return !temp;
            }
        }).cache();

        JavaRDD<LabeledPoint> data = rdd.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String v1) throws Exception {
                String[] arr = v1.split(DMMConstants.COLNUM_DELIMETER_COMMA);
                Integer[] temp = needChangeToNumCol.toArray(new Integer[]{});
//                默认最后一列作为目标分类
                int label_index = arr.length - 1;
                for (int i = 0; i < temp.length; i++) {
                    if (label_index != i) { // 排除目标列
                        int index = temp[i];
                        arr[index] = featuresMap.get(arr[index].trim()).toString();
                    }
                }
//              目标分类
//                TODO  删除下一行代码 分类规则有用户确定 程序不做数据分类  分类规则解析 由用户确定 程序不作处理
                arr[label_index] = Integer.valueOf(arr[label_index]).intValue() < 10 ? "1" : "0";
//                arr[label_index] = classifiLable(arr[label_index], classificationRule).toString();

                double label = Double.valueOf(arr[arr.length - 1]);
                double[] features = new double[arr.length - 1];

                for (int i = 0; i < arr.length - 1; i++) {
                    features[i] = Double.valueOf(arr[i]);
                }
                return new LabeledPoint(label, Vectors.dense(features));
            }
        });

        return data;
    }

    /**
     * 根据分类规则。把指定值的目标值进行预分类
     *  TODO 暂不实现该功能
     * @param value              目标值
     * @param classificationRule 分类规则，目前只支持数字分类
     * @return
     */
    public static Double classifiLable(String value, List<String[]> classificationRule) throws DMMAlgorithmException {
        try {
            boolean isNumStr = value.matches(reg_num);
            if ((classificationRule == null || classificationRule.size() == 0)&&!isNumStr) {
                throw new DMMAlgorithmException("当前lable的目标值：" + value + " 不能转换为Double类型的数据.");
            }else if((classificationRule == null || classificationRule.size() == 0)&&isNumStr){
                return Double.valueOf(value);
            } else if(isNumStr) { // 数字按区间处理
                Double _value = Double.valueOf(value);
                for (int i = 0; i < classificationRule.size(); i++) {
                    String[] rule = classificationRule.get(i);
                    if (StringUtils.isNotBlank(rule[0]) && StringUtils.isNotBlank(rule[1])) {
                        if (Double.valueOf(rule[0]) <= _value && _value <= Double.valueOf(rule[1])) {
                            return Double.valueOf(i);
                        }
                    } else if (StringUtils.isNotBlank(rule[0]) && StringUtils.isBlank(rule[1])) {
                        if (Double.valueOf(rule[0]) <= _value) {
                            return Double.valueOf(i);
                        }
                    } else if (StringUtils.isBlank(rule[0]) && StringUtils.isNotBlank(rule[1])) {
                        if (Double.valueOf(rule[0]) >= _value) {
                            return Double.valueOf(i);
                        }
                    }
                }
            }else if(!isNumStr){
//                TODO 字符串分类处理
            }
        } catch (Exception e) {
            throw new DMMAlgorithmException("当前lable的目标值：" + value + " 不能转换为Double类型的数据.");
        }
        return null;
    }


}
