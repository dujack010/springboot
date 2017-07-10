package com.example.hbase;

import com.example.hbase.pojo.BatchBean;
import com.example.hbase.pojo.CmsResourceProperty;
import com.example.hbase.pojo.SingleBean;
import com.example.hbase.service.HBaseService;
import com.example.hbase.utils.JDBCUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseApplicationTests {
	private static final String itemProfile="item_profile";
	private static final String profileFamily="tag";
	private static final String userProfile="user_profile";
	private static final String scoreFamily="score";
	private static final String scoreTable="score_profile";
	private static Random random = new Random();

	@Autowired
	HBaseService service;

	@Autowired
	JDBCUtil jdbcUtil;

	@Autowired
	SingleBean singleBean;

	@Autowired
	BatchBean batchBean;

	@Test
	public void initAll(){
		try {
			//updateData();
            //long start = System.currentTimeMillis();
            initUserProfile();
			//updateData();
            //long end = System.currentTimeMillis();
            //System.out.println(end-start);
        } catch (Exception e) {
			e.printStackTrace();
		}
		//batchBean.setPutList(putList);
		//service.batchPut(batchBean);
	}

	/**
	 * 向user profile填充数据
	 */
	private void initUserProfile() throws InterruptedException {
		//获取所有标签id
		String sql = "SELECT property_id FROM comm_data_property WHERE is_valid = 1";
		List<Long> propertyIdList = jdbcUtil.getIdList(sql, "property_id");
		System.out.println(propertyIdList.size());
		//获取用户id
		sql = "SELECT customer_id FROM customer_auth WHERE state = 1 or state = 2";
		List<Long> customerIdList = jdbcUtil.getIdList(sql, "customer_id");
        //Hbase批量处理
		List<Put> putList = new ArrayList();
		batchBean.setTableName(userProfile);
		singleBean.setTableName(userProfile);
		//遍历用户id作为新rowkey,标签id为column
		byte[] family = Bytes.toBytes(profileFamily);
		int counter = 0;
        System.out.println(customerIdList.size());
        /*for (Long customerId : customerIdList) {
			Put put = new Put(Bytes.toBytes(String.valueOf(customerId)));
			for (Long propertyId : propertyIdList) {
				put.addColumn(family,Bytes.toBytes(String.valueOf(propertyId)),Bytes.toBytes(String.valueOf(0)));
			}
            putList.add(put);
            //singleBean.setPut(put);
			//service.put(singleBean);
			counter++;
			if(counter==10){
				System.out.println("凑够10条");
				batchBean.setPutList(putList);
				//service.batchPut(batchBean);
				counter=0;
				System.out.println("休息");
				Thread.sleep(1500);
			}
		}*/
	}

	/**
	 * 向item profile填充数据
	 * 从数据库中读取所有带标签的resource_id
	 * 从数据库中读取所有标签id
	 * rowkey为resource_id, columns为所有标签id,值全部为0
	 * 初始化速度太慢 抛弃
	 */
	private void fillData(){
		String sql = "SELECT CONCAT(resource_id,'/',resource_type) as new_id FROM cms_resource_property WHERE is_valid = 1 GROUP BY resource_id,resource_type having new_id is not null";
		List<String> resourceIdList = jdbcUtil.getStringList(sql, "new_id");
		System.out.println(resourceIdList.size());
		sql = "SELECT property_id FROM comm_data_property WHERE is_valid = 1";
		List<Long> propertyIdList = jdbcUtil.getIdList(sql, "property_id");
		singleBean.setTableName(itemProfile);
		batchBean.setTableName(itemProfile);
		List<Put> putList = new ArrayList();
		byte[] family = Bytes.toBytes(profileFamily);
		int counter = 0;
		for (int i = 0,j=resourceIdList.size(); i < j; i++) {
			Put put = new Put(Bytes.toBytes(resourceIdList.get(i)));
			for (Long propertyId : propertyIdList) {
				put.addColumn(family,Bytes.toBytes(propertyId),Bytes.toBytes(0));
			}
			//putList.add(put);
			/*counter++;
			if(counter==3){
				batchBean.setPutList(putList);
				service.batchPut(batchBean);
				counter = 0;
			}*/
			singleBean.setPut(put);
			service.put(singleBean);
		}
	}

	/**
	 * 按资源id和类型分组
	 * 资源id/资源类型作为新的id,组内所有标签id以","分割拼串
	 * 向hbase item_profile表初始化数据，只存储资源下已有的标签列
	 */
	private void updateData() throws InterruptedException {
//		String sql = "SELECT resource_id,GROUP_CONCAT(property_id SEPARATOR ',') as property_id_list FROM cms_resource_property WHERE is_valid = 1 AND resource_id is not null GROUP BY resource_id";
		String sql = "SELECT CONCAT(resource_id,'/',resource_type) as new_id,GROUP_CONCAT(DISTINCT(property_id) SEPARATOR ',') as property_id_list FROM cms_resource_property WHERE is_valid = 1 GROUP BY resource_id,resource_type HAVING new_id is not NULL";
		List<CmsResourceProperty> rpList = jdbcUtil.getRpList(sql);
		byte[] family = Bytes.toBytes(profileFamily);
		List<Put> putList = new ArrayList<>();
		batchBean.setTableName(itemProfile);
		for (CmsResourceProperty crp : rpList) {
			String resourceId = crp.getResourceId();
			String[] propertyIdList = crp.getPropertyIdList().split(",");
			if(propertyIdList.length==0){
				System.out.println("该资源无对应标签："+resourceId);
				continue;
			}
			Put put = new Put(Bytes.toBytes(resourceId));
			for (String s : propertyIdList) {
				put.addColumn(family,Bytes.toBytes(s),Bytes.toBytes(1));
			}
			putList.add(put);
			if(putList.size()>=200){
				System.out.println("批量条数:"+putList.size());
				batchBean.setPutList(putList);
				service.batchPut(batchBean);
				putList.clear();
				System.out.println("批量完毕，清理list后长度："+putList.size());
			}
		}
	}

	private void checkList(List<Long> list){
		System.out.println("check size:"+list.size());
		for (Long a : list) {
			System.out.println("check value:"+a);
		}
	}

	/**
	 * 批量写入测试
	 * @throws InterruptedException
	 */
	@Test
	public void createItemProfile() throws InterruptedException {
		//列代表资源的某种属性，此处为标签
		//向itemProfile表中写入1W行数据，每行10W列属性
		singleBean.setTableName(itemProfile);
		List<Put> putList = new ArrayList();
		byte[] family = Bytes.toBytes(profileFamily);
		//rowkey为1-10000自增数
		long start = System.currentTimeMillis();
		for (int i = 1; i < 1000; i++) {
			Put put = new Put(Bytes.toBytes(i));
			for (int j = 1; j < 13000; j++) {
				//每一列数据为0或1，表示有无
				put.addColumn(family,Bytes.toBytes(j+"t"),Bytes.toBytes(random.nextInt(2)));
			}
			singleBean.setPut(put);
			service.put(singleBean);
		}
		/*batchBean.setPutList(putList);
		service.batchPut(batchBean);*/
		long end = System.currentTimeMillis();
		System.out.println("写入1000条完成,用时:"+(end-start));
	}

	@Test
	public void getTest() {
		//rowkey: 1497578016241
		//耗时6秒
		singleBean.setTableName(scoreTable);
		Get get = new Get(Bytes.toBytes(1397618593768L));
		get.addFamily(Bytes.toBytes(profileFamily));
		get.setFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(1))));
		singleBean.setGet(get);
        Result result = service.get(singleBean);
        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(profileFamily));
		System.out.println(familyMap.size());
		Set<Map.Entry<byte[], byte[]>> entries = familyMap.entrySet();
		for (Map.Entry<byte[], byte[]> entry : entries) {
			System.out.println(Bytes.toLong(entry.getKey()));
			System.out.println(Bytes.toInt(entry.getValue()));
		}
		//byte[] value = result.getValue(baseBean.getFamilyName().getBytes(), Bytes.toBytes("1497578016242"));
        //System.out.println(Bytes.toString(value));
	}

    @Test
    public void updateTest() {
        //rowkey: 1497578016241
        //耗时6秒
        singleBean.setTableName(itemProfile);
        Put put = new Put(Bytes.toBytes(1497578016241L));
        put.addColumn(Bytes.toBytes(profileFamily),Bytes.toBytes(1497578016242L),Bytes.toBytes(100));
        singleBean.setPut(put);
        long start = System.currentTimeMillis();
        service.put(singleBean);
        long end = System.currentTimeMillis();
        System.out.println(end-start);
    }

	/**
	 * 依次写入测试
	 */
	@Test
	public void saveTest() {
		//rowkey: 1497578016241
		//耗时6秒
		singleBean.setTableName("test");
		for (int i = 0; i < 1; i++) {
			singleSave(singleBean);
		}
	}

	private void singleSave(SingleBean singleBean){
		byte[] family = Bytes.toBytes("test");
		long customerId = System.currentTimeMillis();
		Put put = new Put(Bytes.toBytes("key2"));
		Random r = new Random();
		long start = System.currentTimeMillis();
		for (int i= 1;  i< 2; i++) {
			put.addColumn(family,Bytes.toBytes(i+""),Bytes.toBytes("2"));
			put.addColumn(family,Bytes.toBytes(i+""),Bytes.toBytes("3"));
		}
		singleBean.setPut(put);
		service.put(singleBean);
		long end = System.currentTimeMillis();
		System.out.println("流程用时："+(end-start));
	}
}
