package com.cdcalabar.dmm.utils;

import com.cdcalabar.dmm.context.DMMContext;
import com.cdcalabar.dmm.serializer.BaseSerializble;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by lkl on 16-8-3.
 */
public class DMMHDFSUtils extends BaseSerializble{

    /**
     * spark删除hdfs目录
     * @param path
     * @throws IOException
     */
    public static void deletePath(String path) throws IOException{
        Configuration hadoopConf = DMMContext.sc.hadoopConfiguration();
        FileSystem hdfs = FileSystem.get(hadoopConf);
        Path  _path = new Path(path);
        if(hdfs.exists(_path)){
            //为防止误删，禁止递归删除
            hdfs.delete(_path,true);
        }
    }
}
