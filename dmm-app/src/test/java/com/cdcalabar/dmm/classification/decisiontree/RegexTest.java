package com.cdcalabar.dmm.classification.decisiontree;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2016/7/19 0019.
 */
public class RegexTest {

    @Test
    public void test(){
        String regex = "^(\\-|\\+)?\\d+(\\.\\d+)?$";
        System.out.println("12".matches(regex));
        System.out.println("10.33".matches(regex));
        System.out.println("0.23".matches(regex));
        System.out.println("-0.23".matches(regex));
        System.out.println("-190.23".matches(regex));
        System.out.println("+0.23".matches(regex));
        System.out.println("+110.23".matches(regex));
    }

    @Test
    public void test1(){
//        String[] classificationRule = {"[0-4]", "[5-9]", "[10-15]","[16-60]"};
//        String[] classificationRule = {"1-0-4","2-5-9","3-10-15","4-16-20"};
        int value = 3;
        List<String[]> classificationRule = new ArrayList<String[]>();
        classificationRule.add(new String[]{"0","4"});
        classificationRule.add(new String[]{"5","9"});
        for(int i = 0 ;i<classificationRule.size();i++){
            String[] rule = classificationRule.get(i);
            if(Double.valueOf(rule[0])<=value && value<=Double.valueOf(rule[1])){
                System.out.println(i);
                continue;
            }
        }
    }
}
