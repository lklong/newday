package com.cdcalabar.dmm.serializer;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;

/**
 * Created by Administrator on 2016/7/12 0012.
 * 序列化接口
 */
public class BaseSerializble implements KryoRegistrator {
    @Override
    public void registerClasses(Kryo kryo) {

    }
}
