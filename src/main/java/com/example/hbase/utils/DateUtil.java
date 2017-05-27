package com.example.hbase.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by DuJunchen on 2017/4/19.
 */
public class DateUtil {
    public static String getRowkey(String date){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            return sdf.parse(date).getTime()+"_";
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }
}
