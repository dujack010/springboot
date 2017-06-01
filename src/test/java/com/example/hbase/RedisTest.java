package com.example.hbase;

import com.example.hbase.utils.RedisUtil;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Set;

/**
 * Created by DuJunchen on 2017/6/1.
 */
public class RedisTest {

    private RedisUtil util = new RedisUtil();

    @Test
    public void test1(){
        String prefix = "a:customer:recommend:resource:all:";
        String customerId = "1435146439349";
        String key = prefix + customerId;
        JedisPool client = util.getPool();
        Jedis jedis = client.getResource();
        Set<String> zrange = jedis.zrange(key, 0L, -1);
        for (String s : zrange) {
            System.out.println(s);
        }
        jedis.close();
    }
}
