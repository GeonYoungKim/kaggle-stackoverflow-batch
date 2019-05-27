package com.skuniv.cs.geonyeong.kaggle.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TimeUtil {

    private static final SimpleDateFormat baseFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Date toDate(String date) throws ParseException {
        return baseFormat.parse(date);
    }

    public static Date toDate(Date date) throws ParseException {
        return baseFormat.parse(baseFormat.format(date));
    }

    public static Date toDate(String date, SimpleDateFormat format) throws ParseException {
        return format.parse(date);
    }

    public static String toStr(Date date) {
        return baseFormat.format(date);
    }

    public static String toStr(Date date, SimpleDateFormat format) {
        return format.format(date);
    }

    public static String toStr(String date) throws ParseException {
        return baseFormat.format(baseFormat.parse(date));
    }
}
