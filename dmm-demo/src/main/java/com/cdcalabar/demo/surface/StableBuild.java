package com.cdcalabar.demo.surface;


import com.cdcalabar.demo.env.SparkENV;
import com.cdcalabar.demo.env.constants.ENVConstants;
import com.cdcalabar.demo.io.StablityResult;
import com.cdcalabar.demo.logistic.StableCalculate;
import com.cdcalabar.demo.monitor.Monitor;
import com.cdcalabar.demo.params.StableParams;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class StableBuild {

    private static final Logger LOGGER = LoggerFactory.getLogger(StableBuild.class.getName());

    public static void buildStableCaculate(String input,String output,String appName,String master,double acquisitionFrequency,Integer acquisitionPoint){

        try {
//        1.参数验证
            StableParams.validateParams();
//        2.环境初始化
            JavaSparkContext sc =  SparkENV.initSC(appName,master);
//        3.构建输入RDD
            JavaRDD<String> inputRDD = StablityResult.initRDD(sc,input);
//        4.调用算子计算
            JavaRDD<String> outputRDD = StableCalculate.calculateStablity(sc,inputRDD,acquisitionFrequency,acquisitionPoint);
//        5.存储
            StablityResult.output(output,outputRDD);
        }catch (Exception e){
//          异常监控 通知it程序异常，及时处理
            LOGGER.error("程序异常"+e);
            Monitor.monitorExceHandler();
        }

//        返回通知用户已做完计算


    }

    public static void main(String[] args) {

        String hdfs = ENVConstants.pps.getProperty("hdfs_ip")+":"+ENVConstants.pps.getProperty("hdfs_port");
        String input = hdfs+"/user/Administrator/data/data.txt";
        String output = hdfs+"/user/Administrator/data/output/stable_res_3";
        String appName = ENVConstants.pps.getProperty("app_name");
        String master = ENVConstants.pps.getProperty("master_ip");
        double acquisitionFrequency = 2000d;
        int acquisitionPoint = 40000;

        buildStableCaculate(input,output,appName,master,acquisitionFrequency,acquisitionPoint);
    }

}
