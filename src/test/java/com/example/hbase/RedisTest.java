package com.example.hbase;

import com.example.hbase.utils.RedisClusterUtil;
import com.example.hbase.utils.RedisParam;
import com.example.hbase.utils.RedisUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by DuJunchen on 2017/6/1.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisTest {

    private static final String redisPrefix_video_property_sort = RedisParam.R_VIDEO_PROPERTY_SORT;
    private static final String redisPrefix_doc_property_sort = RedisParam.R_DOC_PROPERTY_SORT;
    private static final String redisPrefix_case_property_sort = RedisParam.R_CASE_PROPERTY_SORT;
    private static final String redisPrefix_property_resource_sort = RedisParam.R_PROPERTY_RESOURCE_SORT;
    private static final String redisPrefix_video_dao = RedisParam.R_VIDEO_BASEINFO;
    private static final String redisPrefix_doc_dao = RedisParam.R_DOC_BASEINFO;
    private static final String redisPrefix_case_dao = RedisParam.R_CASE_BASEINFO;
    private static final String redisPrefix_all = RedisParam.R_CUSTOMER_RECOMMEND_RESOURCE_ALL;
    private static final String redisPrefix_customer_browse = RedisParam.R_CUSTOMER_BROWSE_RECENT;
    private RedisUtil util = new RedisUtil();

    //redis集群配置
    @Autowired
    RedisClusterUtil clusterUtil;

    @Autowired
    RedisTemplate<String,String> redisTemplate;

    @Test
    public void clusterTest(){
        ZSetOperations<String, String> op = redisTemplate.opsForZSet();
        //Double score = zSetOperations.score("recommend:customer:score:profile:1397586886349", "1418139699975");
        Set<ZSetOperations.TypedTuple<String>> typedTuples = op.rangeWithScores("recommend:customer:score:profile:1397586886349", 0, -1);
        List<String> scoredIdList = new ArrayList<>();
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            scoredIdList.add(typedTuple.getValue());
        }

    }

    @Test
    public void test1(){
        Jedis resource = util.getPool().getResource();
        resource.zadd("recommendTestKey",0.0,"a");
        Double zscore = resource.zscore("recommendTestKey", "a");
        System.out.println(zscore);
    }
}
