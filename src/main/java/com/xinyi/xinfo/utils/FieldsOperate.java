package com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FieldsOperate {

    public static String getFields(JSONObject jsonObject,String tableName){
        StringBuilder result = new StringBuilder();
        int count = 0 ;
        List<String> addFields = new ArrayList<>();

        //获取表字段
        List<String> fields = TableOperate.getFields(tableName);

        for (Map.Entry<String, Object> entry : jsonObject.entrySet()) {
            count++;
            String key = entry.getKey() ;
            if(count != jsonObject.entrySet().size()) {
                if (key.equals("id")) {
                    result.append("_id").append(",");
                } else if (key.equals("data")) {
                    result.append("_data").append(",");
                } else if (key.equals("user")) {
                    result.append("_user").append(",");
                } else {
                    result.append(key).append(",");
                }
            }else{
                if (key.equals("id")) {
                    result.append("_id");
                } else if (key.equals("data")) {
                    result.append("_data");
                } else if (key.equals("user")) {
                    result.append("_user");
                } else {
                    result.append(key);
                }
            }

            //判断数据里的字段是否包含在表中字段里，不包含的话，往表中新增字段
           boolean bool =  fields.contains(entry.getKey().toLowerCase());
            if(!bool){
                addFields.add(entry.getKey());
            }
        }
        if(addFields.size() != 0 ){
            TableOperate.addFields(tableName,addFields);
        }
        return result.toString() ;
    }
}
