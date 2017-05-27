package com.example.hbase;

import com.example.hbase.pojo.BaseBean;
import com.example.hbase.service.HBaseService;
import com.example.hbase.utils.DateUtil;
import com.example.hbase.utils.KafkaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HbaseApplicationTests {

	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	HBaseService service;

	@Autowired
	KafkaUtil util;

	@Autowired
	BaseBean baseBean;

	@Test
	public void saveTest() {
		baseBean.setTableName("testTable");
		baseBean.setFamilyName("test");
		baseBean.setRowKey(System.currentTimeMillis()+"");
		Map column = new HashMap();
		column.put("value1",34567);
		column.put("value2",34567);
		baseBean.setColumn(column);
		service.testPut(baseBean);
	}

	@Test
	public void scanTest(){
		baseBean.setTableName("log_track");
		baseBean.setFamilyName("log_track_family");
		String startRow = getStartOrEndKey(DateUtil.getRowkey("2017-04-18 00:00:00"),1);
		String endRow = getStartOrEndKey(DateUtil.getRowkey("2017-04-18 23:59:59"),2);
		baseBean.setStartRow(startRow);
		baseBean.setEndRow(endRow);
		ResultScanner scanner = service.scan(baseBean);
		Iterator<Result> iterator = scanner.iterator();
		KafkaTemplate template = util.getTemplate();
		while (iterator.hasNext()){
			Result next = iterator.next();
			NavigableMap<byte[], byte[]> familyMap = next.getFamilyMap(baseBean.getFamilyName().getBytes());
			Set<Map.Entry<byte[], byte[]>> entries = familyMap.entrySet();
			StringBuilder sb = new StringBuilder();
			for (Map.Entry<byte[], byte[]> entry : entries) {
				String msg = Bytes.toString(entry.getKey()) + ":" + Bytes.toString(entry.getValue());
				sb.append(msg + "\t");
			}
			template.send("dataClean",sb.toString());
			//template.send("cleanData",next.toString());
			//byte[] value = next.getValue(baseBean.getFamilyName().getBytes(), "sessionId".getBytes());
		}
	}

	private String getStartOrEndKey(String time, int i){
		if(!time.isEmpty()){
			if(i==1){
				//start rowKey
				return time+"0";
			}else {
				//end rowKey
				return time+"9999";
			}
		}else{
			return "";
		}
	}
}
