package com.example.hbase.service;

import com.example.hbase.admin.HBaseAdmin;
import com.example.hbase.pojo.BatchBean;
import com.example.hbase.pojo.SingleBean;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.springframework.stereotype.Service;

/**
 * Created by DuJunchen on 2017/4/14.
 */
@Service
public class HBaseService extends HBaseAdmin {

    public void put(SingleBean bean) {
        super.put(bean);
    }

    public Result get(SingleBean bean) {
        return super.get(bean);
    }

    public ResultScanner scan(SingleBean bean){
        return super.scan(bean);
    }

    public void batchPut(BatchBean bean){
        super.batchPut(bean);
    }
}
