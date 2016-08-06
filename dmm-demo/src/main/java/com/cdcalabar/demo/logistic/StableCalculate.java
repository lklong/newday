package com.cdcalabar.demo.logistic;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class StableCalculate {

    /**
     * 斯佩林舒适度计算
     * @param acquisitionFrequency 计算频率
     * @param acquisitionPoint 采集点数
     * @return
     */
    public static  JavaRDD<String> calculateStablity(JavaSparkContext sc,JavaRDD<String> inputRDD,double acquisitionFrequency,Integer acquisitionPoint) {

        final List<String> tempFile = inputRDD.collect();
        final String[] file =  tempFile.toArray(new String[tempFile.size()]);

        double[] f = new double[acquisitionPoint];
        final double[] F = new double[acquisitionPoint];
        List<Integer> keys  = new ArrayList<Integer>();

        int n = 0;
        while (n < acquisitionPoint){
            keys.add(n);
            f[n] = n*(acquisitionFrequency/acquisitionPoint);
            if(0.5<=f[n] && f[n]<5.9){
                F[n]=0.325*f[n];
            }else if(5.9<=f[n] && f[n]<=20){
                F[n]=400/f[n];
            }else if(f[n]>20){
                F[n]= 1d/f[n];
            }else {
                F[n] = 0d;
            }
            n++;
        }

        JavaRDD<Integer> F_Key = sc.parallelize(keys).persist(StorageLevel.MEMORY_AND_DISK_2());
        JavaPairRDD<Integer,Double> F_Data = F_Key.mapToPair(new PairFunction<Integer, Integer, Double>() {
            public Tuple2<Integer, Double> call(Integer x) throws Exception {
                return new Tuple2<Integer, Double>(x,F[x]);
            }
        }).partitionBy(new HashPartitioner(2)).persist(StorageLevel.MEMORY_AND_DISK_2());

        JavaPairRDD<Integer,Double> aFile = F_Key.mapToPair(new PairFunction<Integer, Integer, Double>() {
            public Tuple2<Integer, Double> call(Integer x) throws Exception {
                return new Tuple2<Integer, Double>(x,Double.valueOf(file[x]));
            }
        }).mapValues(new Function<Double, Double>() {
            public Double call(Double v1) throws Exception {
                return Math.pow(v1,3);
            }
        }).partitionBy(new HashPartitioner(2)).persist(StorageLevel.MEMORY_AND_DISK_2());

        Double r = aFile.join(F_Data).mapValues(new Function<Tuple2<Double, Double>, Double>() {
            public Double call(Tuple2<Double, Double> x) throws Exception {
                return 7.08 * Math.pow(x._1 * x._2, 0.1);
            }
        }).values().reduce(new Function2<Double, Double, Double>() {
            public Double call(Double v1, Double v2) throws Exception {
                return Math.pow(Math.pow(v1, 10) + Math.pow(v2, 10), 0.1);
            }
        });

        return  sc.parallelize(Arrays.asList("result\t"+r));
    }
}
