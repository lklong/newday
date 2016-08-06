package com.cdcalabar.demo.params;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/7 0007.
 */
public class StableParams {

    /**
     * 参数验证（验证内容待完善）
     * @return
     */
    public static void validateParams(){
        Map<String,String> validateRes = new HashMap<String,String>();

//        验证过程 略

        if(validateRes.size()!=0){
             throw new RuntimeException("参数验证不通过");
       }
    }

}
