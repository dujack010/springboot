package com.example.hbase.pojo;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Created by DuJunchen on 2017/4/13.
 */
@Component
@Scope("prototype")
public class BaseBean {
    private String rowKey;
    private String startRow;
    private String endRow;
    private String tableName;
    private String familyName;
    private Map column;

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public Map getColumn() {
        return column;
    }

    public void setColumn(Map column) {
        this.column = column;
    }

    public BaseBean() {
    }

    public String getStartRow() {
        return startRow;
    }

    public void setStartRow(String startRow) {
        this.startRow = startRow;
    }

    public String getEndRow() {
        return endRow;
    }

    public void setEndRow(String endRow) {
        this.endRow = endRow;
    }
}
