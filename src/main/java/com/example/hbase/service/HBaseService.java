package com.example.hbase.service;

import com.example.hbase.admin.HBaseAdmin;
import com.example.hbase.pojo.BaseBean;
import org.springframework.stereotype.Service;

/**
 * Created by DuJunchen on 2017/4/14.
 */
@Service
public class HBaseService extends HBaseAdmin {
    public void testPut(BaseBean bean) {
        super.put(bean);
    }

    public void testScan(BaseBean bean){
        super.scan(bean);
    }
}
