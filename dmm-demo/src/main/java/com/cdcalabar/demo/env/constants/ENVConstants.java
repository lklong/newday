package com.cdcalabar.demo.env.constants;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 存放环境信息 从env.propertties文件中读取
 * Created by Administrator on 2016/6/30 0030.
 */
public class ENVConstants {

    public final static Properties pps = new Properties();

    static {
        try {
//            InputStream in = new BufferedInputStream(new FileInputStream("env.properties"));
//            pps.load(in);
            InputStream is = new FileInputStream("E:\\IdeaProjects\\hadoopbook\\myScala\\src\\main\\java\\com\\calabar\\demo\\env\\constants\\env.properties");
            pps.load(is);
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(pps.get("master_ip"));
    }


}
