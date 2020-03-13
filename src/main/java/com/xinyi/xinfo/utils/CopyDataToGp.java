package com.xinyi.xinfo.utils;

import com.alibaba.fastjson.JSONObject;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

/**
 * JDBC写数据到文件中再Copy到postgresql中,
 * 经过测试，大概写入12万条数据2秒+左右
 */
public class CopyDataToGp {

    public static final Logger logger = LoggerFactory.getLogger(CopyDataToGp.class);
    private static List<String> fieldsList = new ArrayList<>() ;

    //数据信息一样时使用
//    public static String writeFile(List<JSONObject> list){
//        FileWriter out = null;
//        String filePath = "./"+UUID.randomUUID();
//
//        try{
//            out = new FileWriter(new File(filePath));
//            for(int i=0;i<list.size();i++){
//                Object[] objs = list.get(i).values().toArray();
//                for(int j=0;j<objs.length;j++){
//                    if(objs[j] == null){
//                        out.write("null");
//                    }else{
//                        String str = String.valueOf(objs[j]).replace("\n","") ;
//                        out.write(str);
//                    }
//                    if(j != objs.length - 1){
//                        out.write("^");
//                    }
//                }
//                if(i != list.size() - 1){
//                    out.write("\n");
//                }
//            }
//            out.flush();
//        }catch(Exception ex){
//            ex.printStackTrace();
//        }finally{
//            if(out != null){
//                try {
//                    out.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        return filePath;
//    }

    /**
     *  字段数不统一时处理方法，比如中间某些数据跟第一条数据不一样时:
     *  1、字段数比第一条数据字段数少时，其他缺失字段数值设置为0
     *  2、字段数比第一条数据字段数多时，先往目标表中新增字段，然后再插入数据（忽略已插入的数据）
     *
     *  文件写入主要问题：1、字段缺少 2、数据有换行符，有空格 3、有乱码00x0,0x80,0x90 4、字段数据太长
     *
     */
    public static String writeFile(List<JSONObject> list,String tableColumns,String tableName,Connection conn){
        FileWriter out = null;
        String filePath = "./"+UUID.randomUUID();
        //BufferedWriter out = null ;
        Map<String,Integer> fieldsLength = getFieldsLength(tableName,conn);
        List<Integer> lens = new ArrayList<>();

        try{
            out = new FileWriter(new File(filePath));
            String[] fields = tableColumns.split(",") ;
            for (String field : fields) {
                lens.add(fieldsLength.get(field.toLowerCase()));
            }

            for(int i=0;i<list.size();i++){
                Object[] objs = list.get(i).values().toArray();
                if(objs.length != fields.length){
                    //判断每条数据长度是否一样，不一样做处理，缺少的值设置为0
                    List<Object> objsTemp = new ArrayList<>();
                        for(Object obj : objs){
                            objsTemp.add(obj);
                        }
                        for (int j = 0; j < fields.length; j++) {
                            boolean b = list.get(i).containsKey(fields[j]);
                            if(!b){
                                objsTemp.add(j,0);
                            }
                        }


                    for(int j=0;j<objsTemp.size();j++){
                        int len = objsTemp.get(j).toString().length();
                        if(len> lens.get(j) ){
                            System.out.println(len);
                            System.out.println(lens.get(j));
                            TableOperate.updateFields(tableName,fields[j].toLowerCase(),3000);
                            fieldsLength = getFieldsLength(tableName,conn);
                            for(Map.Entry<String,Object> entry:list.get(1).entrySet()){
                                lens.add(fieldsLength.get(entry.getKey().toLowerCase()));
                            }
                        }
                        if(objsTemp.get(j) == null){
                            out.write("null");
                        }else{
                            String str = String.valueOf(objsTemp.get(j)).replace("\n","")
                                    .replace("\r","").replace("^","")
                                    .replace(" ","").replaceAll("\u0000", "")
                                    .replace("\\","|");
                            out.write(str);
                        }
                        if(j != objsTemp.size() - 1){
                            out.write("^");
                        }
                    }
                }else{
                    for(int j=0;j<objs.length;j++){
                        int len = objs[j].toString().length();
                        //对每个字段数值长度进行判断，如果超过255，则更新该字段长度
                        if(len>lens.get(j)){
                            System.out.println(len);
                            System.out.println(lens.get(j));
                            TableOperate.updateFields(tableName,fields[j].toLowerCase(),3000);
                            fieldsLength = getFieldsLength(tableName,conn);
                            for(Map.Entry<String,Object> entry:list.get(1).entrySet()){
                                lens.add(fieldsLength.get(entry.getKey().toLowerCase()));
                            }
                        }
                        if(objs[j] == null){
                            out.write("null");
                        }else{
                            String str = String.valueOf(objs[j]).replace("\n","")
                                    .replace("\r","").replace("^","")
                                    .replace(" ","").replaceAll("\u0000", "")
                                    .replace("\\","|");
                            out.write(str);
                        }
                        if(j != objs.length - 1){
                            out.write("^");
                        }
                    }
                }
                if(i != list.size() - 1){
                    out.write("\n");
                }
            }
            out.flush();
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(out != null){
                try {
                    out.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return filePath;
    }

    public static long copyData(String tablename, String tableColumns, List<JSONObject> list,Connection conn){
        CopyManager copyManager = null;
        FileReader reader = null;
        String filePath = null ;
        long result = 0 ;

        try{
            filePath = writeFile(list,tableColumns,tablename,conn);

            copyManager = new CopyManager((BaseConnection)conn);
//            reader = new FileReader(new File("./7a5797e6-cf0c-4071-bd23-22dcd758078e"));
            reader = new FileReader(new File(filePath));
            //logger.info("copy "+tablename+" ("+tableColumns+") from stdin delimiter as '^' NULL as 'null'");
            long oneStartTime = System.currentTimeMillis();
            long copyResult = copyManager.copyIn("copy "+tablename+" ("+tableColumns+") from stdin delimiter as '^' NULL as 'null'",reader);
            long oneEndTime = System.currentTimeMillis();
            logger.info("===>> 单次copy插入costTime = "+(oneEndTime-oneStartTime)/1000+" s");
            result = copyResult ;
        }catch(Exception ex){
            ex.printStackTrace();
        }finally{
            if(reader != null){
                try {
                    reader.close();
                    deleteFile(filePath);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return  result ;
    }

    public static int getColumnNum(String tableName,Connection conn){

        String fieldNumSql = "select count(*) from  information_schema.COLUMNS where TABLE_NAME = \'"+tableName+"\'";
        int num = 0;
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(fieldNumSql);
            while (rs.next()) {
                num = rs.getInt(1) ;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return num;
    }

    public static List<String> getFields(String tableName,Connection conn){
        String columnSql = "select COLUMN_NAME from  information_schema.COLUMNS where TABLE_NAME = \'"+tableName+"\'";
        List<String> fields = new ArrayList<>();
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(columnSql);
            while (rs.next()) {
                fields.add(rs.getString(1));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return fields ;
    }

    public static boolean addFields(String tableName, List<String> fields,Connection conn){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < fields.size(); i++) {
            sb.append(" ADD "+fields.get(i)+" varchar(255),");
        }
        String columns = sb.toString().substring(0, sb.toString().lastIndexOf(",")) ;
        String columnSql = "ALTER TABLE "+tableName+columns;
        logger.info(columns);
        logger.info(columnSql);
        try {
            Statement st = conn.createStatement();
            boolean rs = st.execute(columnSql);
            return rs ;
        } catch (Exception e) {
            e.printStackTrace();
            return false ;
        }
    }

    public static Map<String,Integer> getFieldsLength(String tableName ,Connection conn){
        Map<String,Integer> result = new HashMap<>();
        String sql = "select column_name,character_maximum_length from information_schema.columns where table_name= '"+tableName+"'";
        try {
            Statement st = conn.createStatement();
            ResultSet rs = st.executeQuery(sql);
            while (rs.next()) {
                result.put(rs.getString(1),rs.getInt(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result ;
    }

    public static boolean deleteFile(String filePath){
        File file = new File(filePath);
        if(file.exists()){
            file.delete();
            //logger.info("文件已删除!");
            return true ;
        }else{
            logger.info("文件不存在！");
            return false ;
        }
    }

    public static void main(String[] args){
        List<JSONObject> list = new ArrayList<JSONObject>();
        for(int i=0;i<1;i++){
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("testname","收到云台控制请求! 用户ID: 10.200.152.52\n");
            jsonObject.put("id",i);
            jsonObject.put("sex","男\n");
            list.add(jsonObject);
        }

//        copyData("streaminglog_1574991501027", "(msg, overcode, clientid, changroupid, method, typename, playduration, type, username, uri, userid, uuid, ipaddr, logtime, responsecode, duration, gbnumber, applyid, protocol, subclass, remoteaddr)",list);
    }

}
