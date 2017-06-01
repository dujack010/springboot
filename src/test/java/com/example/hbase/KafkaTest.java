package com.example.hbase;

import com.example.hbase.utils.KafkaUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Random;

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
        String video = "1481609642696\t1\t1434613137878\t1";
        String doc = "1481609642696\t2\t1495705843773\t1";
        String topic = "1481609642696\t4\t1491369141509\t1";
        String case1 = "1481609642696\t7\t1429001705499\t1";
        Random random = new Random();
        for (int j = 0; j < 1000; j++) {
            System.out.println("a");
            /*for (int i = 0; i < 100; i++) {
                switch (random.nextInt(4)){
                    case 0: template.send("RecommendCustomerItem_Topic",video); break;
                    case 1: template.send("RecommendCustomerItem_Topic",doc); break;
                    case 2: template.send("RecommendCustomerItem_Topic",topic); break;
                    case 3: template.send("RecommendCustomerItem_Topic",case1); break;
                }
            }*/
            Thread.sleep(1000);
        }

    }
}
