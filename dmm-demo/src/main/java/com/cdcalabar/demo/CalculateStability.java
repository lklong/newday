//package com.cdcalabar.demo;//package com.calabar.demo;
//
//import com.calabar.demo.env.SparkENV;
//import org.apache.spark.HashPartitioner;
//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.Function2;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.storage.StorageLevel;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
///**
// * Created by Administrator on 2016/6/29 0029.
// */
//public class CalculateStability {
////
////    private static  JavaSparkContext sc ;
////
////
////
////    /**
////     * 环境初始化
////     * @param appName
////     * @return
////     */
////    public static JavaSparkContext initSC(String appName,String master){
////        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
////        sc = new JavaSparkContext(conf);
////        return sc;
////    }
////
////    /**
////     * 构建输入RDD
////     * @param input
////     * @param output
////     * @param appName
////     * @param master
////     * @return
////     */
////    public static JavaRDD<String> initRDD(String input,String output,String appName,String master){
////        if (StringUtils.isBlank(master)){
////            master = "local";
////        }
////        sc = initSC(appName,master);
////
////        JavaRDD<String> inputRDD  = sc.textFile(input);
////        return inputRDD;
////    }
//
//
//    /**
//     * 斯佩林舒适度计算
//     * @param input 输入
//     * @param output 输出
//     * @param acquisitionFrequency 计算频率
//     * @param acquisitionPoint 采集点数
//     * @param appName 作业名称
//     * @param master 运行方式
//     * @return
//     */
//    public static  double calculateStablity(String input,String output,double acquisitionFrequency,Integer acquisitionPoint,String appName,String master) {
////       if (StringUtils.isBlank(master)){
////           master = "local";
////       }
////        sc = initSC(appName,master);
////
////        JavaRDD<String> inputRDD  = sc.textFile(input);
//
//        JavaSparkContext sc = SparkENV.initSC(appName,master);
//        JavaRDD<String> inputRDD = SparkENV.initRDD(input);
//        final List<String> tempFile = inputRDD.collect();
//        final String[] file =  tempFile.toArray(new String[tempFile.size()]);
//
//        double[] f = new double[acquisitionPoint];
//        final double[] F = new double[acquisitionPoint];
//        List<Integer> keys  = new ArrayList<Integer>();
//
//        int n = 0;
////        Accumulator<Integer> outFre = sc.accumulator(0);
//        while (n < acquisitionPoint){
//            keys.add(n);
//            f[n] = n*(acquisitionFrequency/acquisitionPoint);
//            if(0.5<=f[n] && f[n]<5.9){
//                F[n]=0.325*f[n];
//            }else if(5.9<=f[n] && f[n]<=20){
//                F[n]=400/f[n];
//            }else if(f[n]>20){
//                F[n]= 1d/f[n];
//            }else {
//                F[n] = 0d;
////                outFre.add(1);
//            }
//            n++;
//        }
//
//
//        JavaRDD<Integer> F_Key = sc.parallelize(keys).persist(StorageLevel.MEMORY_AND_DISK_2());
//        JavaPairRDD<Integer,Double> F_Data = F_Key.mapToPair(new PairFunction<Integer, Integer, Double>() {
//            public Tuple2<Integer, Double> call(Integer x) throws Exception {
////                if(F[x]== null){
////                    System.out.println(x);
////                }
//                return new Tuple2<Integer, Double>(x,F[x]);
//            }
//        }).partitionBy(new HashPartitioner(2)).persist(StorageLevel.MEMORY_AND_DISK_2());
//
//
//        JavaPairRDD<Integer,Double> aFile = F_Key.mapToPair(new PairFunction<Integer, Integer, Double>() {
//            public Tuple2<Integer, Double> call(Integer x) throws Exception {
//                return new Tuple2<Integer, Double>(x,Double.valueOf(file[x]));
//            }
//        }).mapValues(new Function<Double, Double>() {
//            public Double call(Double v1) throws Exception {
//                return Math.pow(v1,3);
//            }
//        }).partitionBy(new HashPartitioner(2)).persist(StorageLevel.MEMORY_AND_DISK_2());
//
//        Double r = aFile.join(F_Data).mapValues(new Function<Tuple2<Double, Double>, Double>() {
//            public Double call(Tuple2<Double, Double> x) throws Exception {
////
////                if(x == null){
////                    System.out.println("here is wrong");
////                    return Double.valueOf(0);
////                }
////                if(x._1 == null ){
////                    System.out.println("here 1 is wrong 75.67354964203494");
////                    return Double.valueOf(0);
////                }
////                if(x._2 == null){
////                    System.out.println("here 2 is wrong 75.67354964203058");
////                    System.out.println(x._1+":"+x._2);
////                    return Double.valueOf(0);
////                }
//                return 7.08 * Math.pow(x._1 * x._2, 0.1);
//            }
//        }).values().reduce(new Function2<Double, Double, Double>() {
//            public Double call(Double v1, Double v2) throws Exception {
//                return Math.pow(Math.pow(v1, 10) + Math.pow(v2, 10), 0.1);
//            }
//        });
//
//
////        sc.parallelize(Arrays.asList("result\t"+r)).saveAsTextFile(output);
//
//        return  r;
//    }
//
//    public static void saveResult(String input,String output,double acquisitionFrequency,Integer acquisitionPoint,String appName,String master){
//        Double r = calculateStablity(input,output,acquisitionFrequency,acquisitionPoint,appName,master);
//        SparkENV.initSC(appName,master).parallelize(Arrays.asList("result\t"+r)).saveAsTextFile(output);
//    }
//
//    public static  void getStabilityResult(){
//        String output = "hdfs://localhost:9000/user/Administrator/data/output/stable_result";
//        SparkConf conf = new SparkConf().setAppName("test").setMaster("local");
//        JavaSparkContext  sc = new JavaSparkContext(conf);
//        JavaRDD<String> result =  sc.textFile(output).map(new Function<String, String>() {
//            @Override
//            public String call(String line) throws Exception {
//                System.out.println(line);
//                return line.split("\t")[1];
//            }
//        });
//
//        System.out.println(result.first());
//
//
//    }
//
//    public static void main(String[] args) {
//        getStabilityResult();
//    }
//}
