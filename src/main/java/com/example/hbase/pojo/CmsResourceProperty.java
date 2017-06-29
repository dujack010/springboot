package com.example.hbase.pojo;

/**
 * Created by DuJunchen on 2017/6/26.
 */
public class CmsResourceProperty {
    private Long resourceId;
    private String propertyIdList;

    public CmsResourceProperty(Long resourceId, String propertyIdList) {
        this.resourceId = resourceId;
        this.propertyIdList = propertyIdList;
    }

    public Long getResourceId() {
        return resourceId;
    }

    public void setResourceId(Long resourceId) {
        this.resourceId = resourceId;
    }

    public String getPropertyIdList() {
        return propertyIdList;
    }

    public void setPropertyIdList(String propertyIdList) {
        this.propertyIdList = propertyIdList;
    }
}
