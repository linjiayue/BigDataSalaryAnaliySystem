package com.lin.schedule.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import com.lin.dao.MainDao;
import com.lin.dao.impl.MainDaoImpl;
import com.lin.schedule.service.ScheduleService;
import com.lin.utils.DateUtils;
import com.lin.utils.HBaseUtils;
import com.lin.utils.LoggerUtils;

public class ScheduleServiceImpl implements ScheduleService {
	private static Logger logger = LoggerUtils.getLogger("ScheduleServiceImpl");
	@Autowired
	private MainDao mainDao;
	
	/**
	 * Add by linjy on 2016-01-06
	 * 根据HBase中的数据，分析每天，每个城市的职位数的数量
	 */
	@Override
	public void analysisDayCityPCount() {
		try {
			StringBuilder sb = new StringBuilder(100);
			sb.append("select count(positionName) as totalCount,city,positionName,createDate from SalaryInfo ");
			//判断SalaryInfoResult是否为空，如果为空，则不添加任何条件，统计所有数据，如果不为空，则只统计当天插入的数据，避免数据重复
			if(!mainDao.tableIsTmpty("SalaryInfoResult")){
				//统计当天插入的数据
				String insertDate = DateUtils.getDateFormat(new Date(), "yyyyMMdd");
				sb.append(" where insertDate = '"+insertDate+"'");
			}
			sb.append(" group by city,positionName,createDate");
		
			logger.info("执行每天，每个城市的职位数的数量分析开始......执行语句为：" + sb.toString());
			List<Map<String, String>> resultLists = mainDao.queryHiveBySql(sb.toString());
			saveDayCityPCount(resultLists);
			logger.info("执行每天，每个城市的职位数的数量分析结果结束......");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("执行每天，每个城市的职位数的数量分析失败，失败原因：" + e.getMessage());
		}
	}
	
	/**
	 * Add by linjy on 2016-01-06
	 * @param totalCount	总数量
	 * @param city	城市名
	 * 保存分析每天，每个城市的职位数的数量的结果到HBase 中
	 */
	private void saveDayCityPCount(List<Map<String, String>> resultLists){
//		String dateTime = DateUtils.getYest0day("yyyyMMdd");
		try{
			Connection conn = HBaseUtils.getConnection();
			Table table = conn.getTable(TableName.valueOf("SalaryInfoResult".getBytes()));
			logger.info("保存每天，每个城市的职位数的数量的结果开始......统计结果数量：" + resultLists.size());
			List<Put> puts = new ArrayList<Put>();
			for(Map<String, String> map : resultLists){
				String totalCount = map.get("totalcount");
				String city = map.get("city");
				String positionName = map.get("positionname");
				String createDate = map.get("createdate");
				String rowKey = createDate + getPostitionNameSX(positionName) + city;
				
				Put put = new Put(rowKey.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "resultCount".getBytes(), totalCount.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "city".getBytes(), city.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "positionName".getBytes(), positionName.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "createDate".getBytes(), createDate.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "insertDate".getBytes(), (DateUtils.getDateFormat(new Date(), "yyyyMMdd")).getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "flag".getBytes(), "0".getBytes());
				
				puts.add(put);
				
//				System.out.println(positionName + "    " + rowKey);
			}
			//插入数据
			table.put(puts);
			//刷新缓冲区
			table.close();
			logger.info("保存每天，每个城市的职位数的数量的结果结束......");
		}catch(Exception e){
			e.printStackTrace();
			logger.error("出入结果数据到HBase总出错，出错原因：" + e.getMessage());
		}
	}
	/**
	 * Add by linjy on 2016-01-07
	 * @param positionName	职位关键词
	 * @return	返回职位关键词缩写
	 */
	private String getPostitionNameSX(String positionName){
		if("Hadoop".equals(positionName)){
			return "hp";
		}
		if("Spark".equals(positionName)){
			return "sp";
		}
		if("数据分析".equals(positionName)){
			return "dm";
		}
		if("数据挖掘".equals(positionName)){
			return "da";
		}
		return "";
	}
	
	public static void main(String[] args) throws Exception {
		StringBuilder sb = new StringBuilder(100);
		sb.append("select count(positionName) as totalCount,city,positionName,createDate from SalaryInfo ");
		sb.append(" group by city,positionName,createDate");
		List<Map<String, String>> resultLists = new MainDaoImpl().queryHiveBySql(sb.toString());
		System.out.println("统计结果数量：" + resultLists.size());
		new ScheduleServiceImpl().saveDayCityPCount(resultLists);
	}

}