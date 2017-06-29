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
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.JedisPool;

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
        String customerId = "1479119582169";
        String resourceId = "1487319405367";
        String type="2";
        JedisPool client = util.getPool();
        Jedis commands = client.getResource();
        long start = System.currentTimeMillis();
        Set<String> browseList = commands.zrevrange(redisPrefix_customer_browse+customerId, 0, -1);
        long e1 = System.currentTimeMillis();
        List<String[]> list = recommendWithBrowse(type, resourceId, customerId, "1", browseList, commands);
        long e2 = System.currentTimeMillis();
        System.out.println(e1-start);
        System.out.println(e2-e1);
        System.out.println(list.size());
        commands.close();
    }

    private List<String[]> recommendWithBrowse(String resourceType, String resourceId, String masterCustomerId, String platformId, Set<String> browseList, JedisCommands commands){
        List<String[]> list = new ArrayList();
        try {
            String resourcePlatformId="1";//默认资源为骨科资源
            // 根据resource_id,resource_type确定key
            // 从redis取property_id,标签treeLevel确定顺序
            Set<String> s = null;
            switch (Integer.parseInt(resourceType)) {
                case 1:
                    s = commands.zrevrange(redisPrefix_video_property_sort + resourceId, 0, -1);
                    break;
                case 2:
                    s = commands.zrevrange(redisPrefix_doc_property_sort + resourceId, 0, -1);
                    break;
                case 7:
                    s = commands.zrevrange(redisPrefix_case_property_sort + resourceId, 0, -1);
                    break;
                default:
                    break;
            }
            if (s != null) {
                int all_num = 0;
                int recommend_case_num = 0;
                int recommend_video_num = 0;
                int recommend_doc_num = 0;
                for (String propertyId : s) {
                    // 根据property_id从redis顺序获取resource_id；
                    if (all_num >= 30) {// 为了计算速度问题，先找出不重复的30个。
                        break;
                    }
                    Set<String> type_resourceList = commands.zrevrange(redisPrefix_property_resource_sort + propertyId, 0,-1);
                    if (type_resourceList != null) {
                        for (String type_resource : type_resourceList) {
                            String[] item = type_resource.split("_");
                            if (item != null && item.length == 2) {
                                String item0 = item[0];
                                String item1 = item[1];
                                if (resourceType.compareTo(item0) == 0 && resourceId.compareTo(item1) == 0) {

                                } else {
                                    String isValid = "";
                                    String tplPath = "";
                                    // 资源有效无效进行判断 去redis查询是否是有效的
                                    switch (Integer.parseInt(item0)) {
                                        case 1:
                                            if (recommend_video_num >= 10) {
                                                continue;
                                            }
                                            isValid = commands.get(redisPrefix_video_dao + item1 + ":" + "isValid");
                                            resourcePlatformId = commands.get(redisPrefix_video_dao + item1 + ":" + "platformId");
                                            break;
                                        case 2:
                                            if (recommend_doc_num >= 10) {
                                                continue;
                                            }
                                            isValid = commands.get(redisPrefix_doc_dao + item1 + ":" + "isValid");
                                            tplPath = commands.get(redisPrefix_doc_dao + item1 + ":" + "tplPath");
                                            resourcePlatformId = commands.get(redisPrefix_doc_dao + item1 + ":" + "platformId");
                                            break;
                                        case 7:
                                            if (recommend_case_num >= 10) {
                                                continue;
                                            }
                                            isValid = commands.get(redisPrefix_case_dao + item1 + ":" + "isValid");
                                            resourcePlatformId = commands.get(redisPrefix_case_dao + item1 + ":" + "platformId");
                                            break;
                                        default:
                                    }
                                    if ("1".equals(isValid)
                                            && tplPath.compareToIgnoreCase("80")!=0  //第三方joa不推荐
                                            && resourcePlatformId!=null && resourcePlatformId.contains(platformId)) {//判断资源是否符合人所属的platform
                                        String[] add_s = new String[3];
                                        add_s[0] = item0;
                                        add_s[1] = item1;
                                        add_s[2] = resourcePlatformId;
                                        String key = redisPrefix_all + masterCustomerId;
                                        String member1 = resourceType + "_" + item1 + "_" + resourcePlatformId;
                                        //String member2 = resourceType + "_" + item1;
                                        /*Double d2 = commands.zscore(key, member2);
                                        if (d2!=null){
                                            commands.zrem(key,member2);
                                        }*/
                                        Double d = commands.zscore(key, member1);
                                        if (d != null && d > 0) {

                                        } else {
                                            // 判断是否浏览
                                            Boolean isBrowse = Boolean.FALSE;
                                            if (browseList != null) {
                                                for (String browse : browseList) {
                                                    String[] browseItem = browse.split("_");
                                                    if (browseItem != null && browseItem.length == 2
                                                            && browseItem[1].equalsIgnoreCase(item1)) {
                                                        isBrowse = Boolean.TRUE;
                                                        break;
                                                    }
                                                }
                                            }
                                            // 为了计算速度问题，先找出不重复的30个。
                                            if (all_num < 30 && !isBrowse) {
                                                list.add(add_s);
                                                all_num++;
                                                switch (Integer.parseInt(item0)) {
                                                    case 1:
                                                        recommend_video_num++;
                                                        break;
                                                    case 2:
                                                        recommend_doc_num++;
                                                        break;
                                                    case 7:
                                                        recommend_case_num++;
                                                        break;
                                                    default:
                                                        break;
                                                }
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            return list;
        }
    }
}
