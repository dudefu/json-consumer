package com.xinyi.xinfo.utils;


import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DateUtil {
    private static Logger logger = LoggerFactory.getLogger(DateUtil.class);
    private static final String DATE_FORMAT = "yyyy-MM-dd";
    private static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
    private static ThreadLocal<Map<String, DateFormat>> dateFormatThreadLocal = new ThreadLocal();
    private static FastDateFormat fdfWithBar = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static FastDateFormat fdfWithNoBar = FastDateFormat.getInstance("yyyyMMddHHmmss");

    public DateUtil() {
    }

    private static DateFormat getDateFormat(String pattern) {
        if (pattern != null && pattern.trim().length() != 0) {
            Map<String, DateFormat> dateFormatMap = (Map)dateFormatThreadLocal.get();
            if (dateFormatMap != null && ((Map)dateFormatMap).containsKey(pattern)) {
                return (DateFormat)((Map)dateFormatMap).get(pattern);
            } else {
                ThreadLocal var2 = dateFormatThreadLocal;
                synchronized(dateFormatThreadLocal) {
                    if (dateFormatMap == null) {
                        dateFormatMap = new HashMap();
                    }

                    ((Map)dateFormatMap).put(pattern, new SimpleDateFormat(pattern));
                    dateFormatThreadLocal.set(dateFormatMap);
                }

                return (DateFormat)((Map)dateFormatMap).get(pattern);
            }
        } else {
            throw new IllegalArgumentException("pattern cannot be empty.");
        }
    }

    public static String formatDate(Date date) {
        return format(date, "yyyy-MM-dd");
    }

    public static String formatDateTime(Date date) {
        return format(date, "yyyy-MM-dd HH:mm:ss");
    }

    public static String format(Date date, String patten) {
        return getDateFormat(patten).format(date);
    }

    public static Date parseDate(String dateString) {
        return parse(dateString, "yyyy-MM-dd");
    }

    public static Date parseDateTime(String dateString) {
        return parse(dateString, "yyyy-MM-dd HH:mm:ss");
    }

    public static Date parse(String dateString, String pattern) {
        try {
            Date date = getDateFormat(pattern).parse(dateString);
            return date;
        } catch (Exception var3) {
            logger.warn("parse date error, dateString = {}, pattern={}; errorMsg = ", new Object[]{dateString, pattern, var3.getMessage()});
            return null;
        }
    }

    public static Date addDays(Date date, int amount) {
        return add(date, 5, amount);
    }

    public static Date addYears(Date date, int amount) {
        return add(date, 1, amount);
    }

    public static Date addMonths(Date date, int amount) {
        return add(date, 2, amount);
    }

    private static Date add(Date date, int calendarField, int amount) {
        if (date == null) {
            return null;
        } else {
            Calendar c = Calendar.getInstance();
            c.setTime(date);
            c.add(calendarField, amount);
            return c.getTime();
        }
    }

    private static String parseSystemCurrentTimeMillis(Long SystemCurrentTimeMillis){
       return DateFormatUtils.format(SystemCurrentTimeMillis,"yyyy-MM-dd HH:mm:ss");
    }

    private static String parseDateTimeToTimeMillis(String dateTime) throws ParseException {
        return fdfWithNoBar.format(fdfWithBar.parse(dateTime));
    }
}
