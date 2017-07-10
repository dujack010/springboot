package com.example.hbase.pojo;

/**
 * Created by DuJunchen on 2017/6/26.
 */
public class CmsResourceProperty {
    private String resourceId;
    private String propertyIdList;

    public CmsResourceProperty(String resourceId, String propertyIdList) {
        this.resourceId = resourceId;
        this.propertyIdList = propertyIdList;
    }

    public String getResourceId() {
        return resourceId;
    }

    public void setResourceId(String resourceId) {
        this.resourceId = resourceId;
    }

    public String getPropertyIdList() {
        return propertyIdList;
    }

    public void setPropertyIdList(String propertyIdList) {
        this.propertyIdList = propertyIdList;
    }
}
