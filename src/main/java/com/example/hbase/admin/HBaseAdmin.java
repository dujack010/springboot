package com.example.hbase.admin;

import com.example.hbase.pojo.BatchBean;
import com.example.hbase.pojo.SingleBean;
import com.example.hbase.utils.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.List;

/**
 * Created by DuJunchen on 2017/4/13.
 */
public class HBaseAdmin {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    HBaseUtil util;

    /**
     * 批量写入
     * @param bean
     */
    public void batchPut(BatchBean bean){
        long start = System.currentTimeMillis();
        List<Put> putList = bean.getPutList();
        Table table = null;
        try {
            table = util.getConnection().getTable(TableName.valueOf(bean.getTableName()));
            table.put(putList);
            long end = System.currentTimeMillis();
            System.out.println("HBASE批量用时:"+(end-start));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 单条数据写入
     */
    public void put(SingleBean bean){
        long start = System.currentTimeMillis();
        Put put = bean.getPut();
        Table table = null;
        try {
            table = util.getConnection().getTable(TableName.valueOf(bean.getTableName()));
            table.put(put);
            long end = System.currentTimeMillis();
            System.out.println("HBASE用时:"+(end-start));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 单条获取数据
     * @param bean
     */
    public Result get(SingleBean bean){
        Get get = bean.getGet();
        Table table = null;
        try {
            table = util.getConnection().getTable(TableName.valueOf(bean.getTableName()));
            Result result = table.get(get);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return new Result();
    }

    /**
     * scan操作
     * @param bean
     * @return
     */
    public ResultScanner scan(SingleBean bean){
        Scan scan = bean.getScan();
        Table table = null;
        try {
            table = util.getConnection().getTable(TableName.valueOf(bean.getTableName()));
            ResultScanner scanner = table.getScanner(scan);
            return scanner;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
