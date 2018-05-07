package com.cetc.database;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Description：
 * Created by luolinjie on 2018/5/2.
 */
public class test {
    private final static Logger logger =  LoggerFactory.getLogger(test.class);
    public static void main(String[] args){
        Map map  = new LinkedHashMap();
        map.put("id",1);
        map.put("name", "tony");
        map.put("age", 26);
        ArrayList<String> list = new ArrayList<String>();
        list.add("id");
        list.add("location");
        list.add("reform_measures");
        map.put("_source",list);
        if (map.size()!=0) {
//            Iterator iterator = filedList.iterator();
//            while (iterator.hasNext()){
//                iterator.next();
//            }


//            JSON.toJSONString(map, true);
            System.out.println(JSON.toJSONString(map, true));
        }

    }
}
