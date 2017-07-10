package com.example.hbase;

import com.example.hbase.utils.KafkaUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by DuJunchen on 2017/4/19.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class KafkaTest {

    @Autowired
    KafkaUtil util;

    @Test
    public void test1() throws InterruptedException {
        KafkaTemplate template = util.getTemplate();
        template.send("dataClean","1");
    }
}
