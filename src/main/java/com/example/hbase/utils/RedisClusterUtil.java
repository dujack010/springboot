package com.example.hbase.utils;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.stereotype.Component;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by DuJunchen on 2017/6/28.
 */
@Component
public class RedisClusterUtil {
    //连接池
    private JedisCluster cluster;

    public JedisCluster getCluster() {
        return cluster;
    }

    public RedisClusterUtil() {
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(1000);
        config.setMaxIdle(100);
        config.setMinIdle(8);
        config.setTestOnBorrow(true);
        config.setTestWhileIdle(true);
        config.setNumTestsPerEvictionRun(10);
        config.setTimeBetweenEvictionRunsMillis(60000);
        config.setMinEvictableIdleTimeMillis(30000);
        cluster = new JedisCluster(setNodes(),2000,2000,5,"admin",config);
    }

    private Set<HostAndPort> setNodes(){
        String hosts = "192.168.1.154:6379,192.168.1.155:6380,192.168.1.156:6380";
        String[] split = hosts.split(",");
        Set<HostAndPort> hostAndPorts = new HashSet<>();
        for (String s : split) {
            System.out.println(s);
            String[] host = s.split(":");
            HostAndPort hostAndPort = new HostAndPort(host[0], Integer.parseInt(host[1]));
            hostAndPorts.add(hostAndPort);
        }
        return hostAndPorts;
    }

}
