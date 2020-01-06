package com.xinyi.xinfo.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class TableOperate {

    private static final Logger logger = LoggerFactory.getLogger(TableOperate.class);
    private static final Connection conn =  GreenPlumUtils.getConnection();

    public static List<String> getFields(String tableName){

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

    public static boolean addFields(String tableName, List<String> fields){
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

    public static boolean updateFields(String tableName,String field,int len){
        boolean bool = true ;
        String columnSql = "ALTER TABLE "+tableName+" alter COLUMN "+field+" type character varying("+len+");";
        logger.info(columnSql);
        try {
            Statement st = conn.createStatement();
            bool = st.execute(columnSql);
        } catch (Exception e) {
            e.printStackTrace();
            bool = false ;
        }
        return bool ;
    }
}
