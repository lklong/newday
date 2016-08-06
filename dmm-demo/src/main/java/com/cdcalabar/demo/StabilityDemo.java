package com.cdcalabar.demo;

import org.apache.spark.Accumulator;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2016/6/29 0029.
 */
public class StabilityDemo {

    public static void main(String[] args) {
//        args = new String[2];
//        args[0] = "hdfs://192.168.1.177:9000/user/hadoop/data/input/stabledata.txt";
//
//        SparkConf conf = new SparkConf().setAppName("StabilityDemo").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("StabilityDemo");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD  = sc.textFile(args[0]);
        final List<String> tempFile = inputRDD.collect();
        final String[] file =  tempFile.toArray(new String[tempFile.size()]);

        double[] f = new double[40000];
        final double[] F = new double[40000];
        double Fs =2000;// 计算频率
        double N = 40000; // 采集点数
        int n = 0;

        List<Integer> F_key_list  = new ArrayList<Integer>();

        // 累加器 统计不在计算频率范围的数据个数
        Accumulator<Integer> outFreNums = sc.accumulator(0);
        while (n < 40000){
            F_key_list.add(n);
            f[n] = n*(Fs/N);
            if(0.5<=f[n] && f[n]<5.9){
                F[n]=0.325*f[n];
            }else if(5.9<=f[n] && f[n]<=20){
                F[n]=400/f[n];
            }else if(f[n]>20){
                F[n]= 1d;
            }else {
                F[n] = 0d;
                outFreNums.add(1);
            }
            n++;
        }

        JavaRDD<Integer> F_Key = sc.parallelize(F_key_list).persist(StorageLevel.MEMORY_AND_DISK_2());
        JavaPairRDD<Integer,Double> F_Data = F_Key.mapToPair(new PairFunction<Integer, Integer, Double>() {
            public Tuple2<Integer, Double> call(Integer x) throws Exception {
//                if(F[x]== null){
//                    System.out.println(x);
//                }
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
//
//                if(x == null){
//                    System.out.println("here is wrong");
//                    return Double.valueOf(0);
//                }
//                if(x._1 == null ){
//                    System.out.println("here 1 is wrong 75.67354964203494");
//                    return Double.valueOf(0);
//                }
//                if(x._2 == null){
//                    System.out.println("here 2 is wrong 75.67354964203058");
//                    System.out.println(x._1+":"+x._2);
//                    return Double.valueOf(0);
//                }
                return 7.08 * Math.pow(x._1 * x._2, 0.1);
            }
        }).values().reduce(new Function2<Double, Double, Double>() {
            public Double call(Double v1, Double v2) throws Exception {
                return Math.pow(Math.pow(v1, 10) + Math.pow(v2, 10), 0.1);
            }
        });

        System.out.println("out-freq:"+outFreNums);
        System.out.println("test-rest:"+ r);
    }
}
