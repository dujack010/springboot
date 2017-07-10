package com.example.hbase.utils;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by DuJunchen on 2017/7/6.
 */
public class HbaseFilterUtil {
    /**
     * 筛选出匹配的所有的行
     */
    public RowFilter getRowFilter(String row){
        return new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(row)));
    }


}
