package com.cdcalabar.dmm.utils;

import com.cdcalabar.dmm.serializer.BaseSerializble;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by lkl on 16-8-2.
 * 字符串工具
 */
public class DMMStringUtils extends BaseSerializble{

    /** 空格 */
    private static final String CONTACT_DEFAULT = "\\x20";

    /**
     * 字符串拼接
     * @param arr 数组
     * @param contact 拼接符号 默认以空格分割
     * @return
     */
    public static String contactArray(String[] arr,String contact){

//        if(StringUtils.isBlank(contact)){
//            contact = CONTACT_DEFAULT;
//        }
        StringBuffer buf = new StringBuffer();
        int len = arr.length;
        for(int i =0 ; i< len ;i++){
            buf.append(arr[i]);
            if(i !=len - 1){
                buf.append(contact);
            }
        }
        return buf.toString();
    }

}
