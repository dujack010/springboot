package com.example.hbase.pojo;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

/**
 * Created by DuJunchen on 2017/4/13.
 */
@Component
@Scope("prototype")
public class SingleBean extends BaseBean {
    private Put put;
    private Get get;
    private Scan scan;

    public Scan getScan() {
        return scan;
    }

    public void setScan(Scan scan) {
        this.scan = scan;
    }

    public Get getGet() {
        return get;
    }

    public void setGet(Get get) {
        this.get = get;
    }

    public void setPut(Put put) {
        this.put = put;
    }


    public Put getPut() {
        return put;
    }
}
