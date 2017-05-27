package com.example.hbase.admin;

import com.example.hbase.pojo.BaseBean;
import com.example.hbase.utils.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by DuJunchen on 2017/4/13.
 */
public class HBaseAdmin {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    HBaseUtil util;

    /**
     * 单条数据写入
     */
    public void put(BaseBean bean){
        Put put = new Put(bean.getRowKey().getBytes());
        Map column = bean.getColumn();
        Set<Map.Entry> set = column.entrySet();
        for (Map.Entry entry : set) {
            put.addColumn(bean.getFamilyName().getBytes(),entry.getKey().toString().getBytes(),entry.getValue().toString().getBytes());
        }
        Connection connection = util.getConnection();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(bean.getTableName()));
            table.put(put);
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
     * 批量获取数据
     */
    public ResultScanner scan(BaseBean bean){
        byte[] familyName = bean.getFamilyName().getBytes();
        Scan scan = new Scan(bean.getStartRow().getBytes(),bean.getEndRow().getBytes());
        scan.addFamily(bean.getFamilyName().getBytes());
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
