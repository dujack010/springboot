package com.example.hbase.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Created by DuJunchen on 2017/4/19.
 */
@Component
public class KafkaUtil {

    private final KafkaTemplate template;

    @Autowired
    public KafkaUtil(KafkaTemplate template) {
        this.template = template;
    }

    public KafkaTemplate getTemplate(){
        return this.template;
    }
}
