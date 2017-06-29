package com.example.hbase.utils;

import com.example.hbase.pojo.CmsResourceProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by DuJunchen on 2017/6/26.
 */
@Component
public class JDBCUtil {
    @Autowired
    private JdbcTemplate template;

    public List<Long> getIdList(String query,String idName){
        List<Long> list = template.query(query, (rs, rowNum) -> new Long(rs.getLong(idName)));
        return list;
    }

    public List<CmsResourceProperty> getRpList(String query){
        List<CmsResourceProperty> list = template.query(query, (rs, rowNum) -> new CmsResourceProperty(rs.getLong("resource_id"),rs.getString("property_id_list")));
        return list;
    }
}
