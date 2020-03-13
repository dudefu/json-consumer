package com.xinyi.xinfo.utils;

import java.util.Date;

public class Test2 {

    public static void main(String[] args) {

//        System.out.println(DateUtil.formatDateTime(new Date()));

        String str = "JSON" ;
        boolean b1 =  str.equalsIgnoreCase("JSON");
        boolean b2 = str.equalsIgnoreCase("json");
        boolean b3 = str.equalsIgnoreCase("json1");
        System.out.println(b1+" "+b2+" "+b3);
    }
}
