package com.example.hbase.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by DuJunchen on 2017/6/1.
 */
public class RedisUtil {
    private String host = "192.168.1.126";
    private String pwd = "admin";
    private int port = 6379;
    //单点
    private Jedis jedis;
    //连接池
    private JedisPool pool;

    public RedisUtil() {
        JedisPoolConfig config = new JedisPoolConfig();
        pool = new JedisPool(config,host,port,2000,pwd);
    }

    public JedisPool getPool() {
        return pool;
    }
}
