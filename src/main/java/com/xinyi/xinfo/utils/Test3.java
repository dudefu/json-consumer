package com.xinyi.xinfo.utils;


import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;

import java.text.ParseException;
import java.util.Date;

public class Test3 {

    private static FastDateFormat fdfWithBar = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static FastDateFormat fdfWithNoBar = FastDateFormat.getInstance("yyyyMMddHHmmss");

    public static void main(String[] args) throws ParseException {
        //System.out.println();
        Long time1 = System.currentTimeMillis();
        String time = DateFormatUtils.format(time1, "yyyy-MM-dd HH:mm:ss");
        System.out.println(time);
        String rowKeyPassTime = fdfWithNoBar.format(fdfWithBar.parse(time));
        System.out.println(rowKeyPassTime);
        Date date = new Date(System.currentTimeMillis());
        System.out.println(XDateUtils.format(date,"yyyyMMddHHmmss"));

    }
}
