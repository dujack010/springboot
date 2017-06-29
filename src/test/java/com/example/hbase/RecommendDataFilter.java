package com.example.hbase;

import com.example.hbase.pojo.SingleBean;
import com.example.hbase.service.HBaseService;
import com.example.hbase.utils.JDBCUtil;
import com.example.hbase.utils.RedisUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by DuJunchen on 2017/6/19.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class RecommendDataFilter {

    private static final String score="customer_score";
    private static final String scoreTable="score_profile";
    private static final String scoreFamily="score";
    private static final String redis_key_prefix="recommend:customer:score:profile:";

    @Autowired
    HBaseService service;

    @Autowired
    SingleBean singleBean;

    @Autowired
    RedisUtil redisUtil;

    @Autowired
    JDBCUtil jdbcUtil;

    //计算用户对标签的兴趣值
    @Test
    public void updateUserProfile(){
        String customerId = "";
        Jedis jedis = redisUtil.getPool().getResource();
        Set<Tuple> tuples = jedis.zrangeWithScores(customerId, 0, -1);
        int sum = 0;
        List<String> list = new ArrayList<>();
        for (Tuple tuple : tuples) {
            //获取用户评分总分
            sum += tuple.getScore();
            //获取用户打分过的资源id列表
            list.add(tuple.getElement());
        }
        //计算用户打分均分
        double avgScore = sum / tuples.size();
    }

    private double userTagScore(String customerId, double avg){
        Jedis jedis = redisUtil.getPool().getResource();
        //所有涉及该tag且用户评过分的资源列表
        List<String> resourceIdList = new ArrayList<>();
        double sum = 0;
        for (String s : resourceIdList) {
            String key = redis_key_prefix + customerId;
            //从redis获取用户对该资源的评分
            Double score = jedis.zscore(key, s);
            sum += (score - avg);
        }
        double finalScore = sum/resourceIdList.size();
        return finalScore;
    }

    /**
     * 用户打分表存入redis
     */
    @Test
    public void customerScoreToRedis(){
        Scan scan = new Scan();
        scan.setFilter(new ColumnPrefixFilter(Bytes.toBytes("scores")));
        singleBean.setTableName(score);
        singleBean.setScan(scan);
        ResultScanner results = service.scan(singleBean);
        JedisPool pool = redisUtil.getPool();
        Jedis resource = pool.getResource();
        Pipeline pipelined = resource.pipelined();
        int counter = 0;
        for (Result result : results) {
            String score = Bytes.toString(result.getValue(Bytes.toBytes(scoreFamily), Bytes.toBytes("scores")));
            String rowKey = Bytes.toString(result.getRow());
            String[] ids = rowKey.split("_");
            String key = redis_key_prefix + ids[0];
            double v = Double.parseDouble(score);
            pipelined.zadd(key,v,ids[1]);
            counter++;
            if(counter==2000){
                System.out.println("批量"+counter+"条");
                long start = System.currentTimeMillis();
                pipelined.sync();
                counter=0;
                long end = System.currentTimeMillis();
                System.out.println("批量结束,用时："+(end-start));
                System.out.println("counter复原"+counter);
            }
        }
    }

    /**
     * 对用户打分表重新排列
     * @param
     */
    @Test
    public void customerScoreReset(){
        Scan scan = new Scan();
        scan.setFilter(new KeyOnlyFilter());
        singleBean.setScan(scan);
        singleBean.setTableName(score);
        ResultScanner scanResult = service.scan(singleBean);
        Set<String> set = new HashSet<>();
        //获取所有有评分记录的customerId
        for (Result next : scanResult) {
            String rowKey = Bytes.toString(next.getRow());
            String customerId = rowKey.substring(0, 13);
            set.add(customerId);
        }
        System.out.println("用户总数："+set.size());
        long begin = System.currentTimeMillis();
        for (String customerId : set) {
            long start = System.currentTimeMillis();
            dataInitHbase(customerId);
            long end = System.currentTimeMillis();
            System.out.println("用户:"+customerId+"打分初始化完成,用时:"+(end-start));
        }
        long fina = System.currentTimeMillis();
        System.out.println("打分初始化完成,总用时:"+(fina-begin));
    }

    private void dataInitHbase(String customerId){
        Scan scan = new Scan();
        FilterList filterList = getFilterList(customerId);
        scan.setFilter(filterList);
        singleBean.setScan(scan);
        singleBean.setTableName(score);
        ResultScanner result = service.scan(singleBean);
        Put put = new Put(Bytes.toBytes(customerId));
        for (Result next : result) {
            String s = Bytes.toString(next.getRow());
            String[] split = s.split("_");
            String score = Bytes.toString(next.getValue(Bytes.toBytes(scoreFamily), Bytes.toBytes("scores")));
            put.addColumn(Bytes.toBytes(scoreFamily),Bytes.toBytes(split[1]),Bytes.toBytes(score));
        }
        singleBean.setPut(put);
        singleBean.setTableName(scoreTable);
        service.put(singleBean);
    }

    private FilterList getFilterList(String prefix){
        List<Filter> filters = new ArrayList<>();
        PrefixFilter pf = new PrefixFilter(Bytes.toBytes(prefix));
        filters.add(pf);
        Filter cpf = new ColumnPrefixFilter(Bytes.toBytes("scores"));
        filters.add(cpf);
        return new FilterList(FilterList.Operator.MUST_PASS_ALL, filters);
    }

}
