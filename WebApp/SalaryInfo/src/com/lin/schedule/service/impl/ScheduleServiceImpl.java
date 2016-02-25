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
			if(!mainDao.tableIsTmptyByFlag("SalaryInfoResult", "ResultInfoFamily", "0")){
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
				//rowKey 增加存储类型标志位
				String rowKey = createDate + getPostitionNameSX(positionName) + city + "0";
				
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
			logger.error("保存每天，每个城市的职位数的数量的结果数据到HBase总出错，出错原因：" + e.getMessage());
		}
	}
	/**
	 * Add by linjy on 2016-01-26
	 * 统计，每天，每个城市，平均工资的数量
	 */
	@Override
	public void analysisDayCitySCount(){
		try {
			StringBuilder sb = new StringBuilder(100);
			sb.append("select count(positionName) as totalCount,city,positionName,createDate,aveValue from SalaryInfo ");
			//判断SalaryInfoResult是否为空，如果为空，则不添加任何条件，统计所有数据，如果不为空，则只统计当天插入的数据，避免数据重复
//			if(!mainDao.tableIsTmpty("SalaryInfoResult")){
			if(!mainDao.tableIsTmptyByFlag("SalaryInfoResult", "ResultInfoFamily", "1")){
				//统计当天插入的数据
				String insertDate = DateUtils.getDateFormat(new Date(), "yyyyMMdd");
				sb.append(" where insertDate = '"+insertDate+"'");
			}
			sb.append(" group by city,positionName,createDate,aveValue");
		
			logger.info("执行统计，每天，每个城市，平均工资的数量开始......执行语句为：" + sb.toString());
			List<Map<String, String>> resultLists = mainDao.queryHiveBySql(sb.toString());
			saveDayCitySCount(resultLists);
			logger.info("执行统计，每天，每个城市，平均工资的数量结束......");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("执行统计，每天，每个城市，平均工资的数量失败，失败原因：" + e.getMessage());
		}
	}
	/**
	 * Add by linjy on 2016-01-26
	 * @param resultLists	分组查询出的结果
	 * 保存统计，每天，每个城市，平均工资的数量结果
	 */
	private void saveDayCitySCount(List<Map<String, String>> resultLists){
		try{
			Connection conn = HBaseUtils.getConnection();
			Table table = conn.getTable(TableName.valueOf("SalaryInfoResult".getBytes()));
			logger.info("保存统计，每天，每个城市，平均工资的数量结果开始......统计结果数量：" + resultLists.size());
			List<Put> puts = new ArrayList<Put>();
			for(Map<String, String> map : resultLists){
				String totalCount = map.get("totalcount");
				String city = map.get("city");
				String positionName = map.get("positionname");
				String createDate = map.get("createdate");
				String aveValue = map.get("avevalue");
				//rowKey 增加存储类型标志位	添加平均工资值避免出现rowKey相同情况
				String rowKey = createDate + getPostitionNameSX(positionName) + city + aveValue + "1";
				
				Put put = new Put(rowKey.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "resultCount".getBytes(), totalCount.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "city".getBytes(), city.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "positionName".getBytes(), positionName.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "aveValue".getBytes(), aveValue.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "createDate".getBytes(), createDate.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "insertDate".getBytes(), (DateUtils.getDateFormat(new Date(), "yyyyMMdd")).getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "flag".getBytes(), "1".getBytes());
				
				puts.add(put);
			}
			//插入数据
			table.put(puts);
			//刷新缓冲区
			table.close();
			logger.info("保存统计，每天，每个城市，平均工资的数量结果结束......");
		}catch(Exception e){
			e.printStackTrace();
			logger.error("存入统计，每天，每个城市，平均工资的数量结果数据到HBase总出错，出错原因：" + e.getMessage());
		}
	}
	
	/**
	 * Add by linjy on 2016-02-22
	 * 统计 每天，每个城市，每个职位的平均工资以及工作年限
	 */
	@Override
	public void analysisDayCitySWCount() {
		try {
			StringBuilder sb = new StringBuilder(100);
			sb.append("select count(positionName) as totalCount,city,positionName,createDate,aveValue,aveWYValue from SalaryInfo ");
			//判断SalaryInfoResult是否为空，如果为空，则不添加任何条件，统计所有数据，如果不为空，则只统计当天插入的数据，避免数据重复
			if(!mainDao.tableIsTmptyByFlag("SalaryInfoResult", "ResultInfoFamily", "1")){
				//统计当天插入的数据
				String insertDate = DateUtils.getDateFormat(new Date(), "yyyyMMdd");
				sb.append(" where insertDate = '"+insertDate+"'");
			}
			sb.append(" group by city,positionName,createDate,aveValue,aveWYValue");
		
			logger.info("执行统计 每天，每个城市，每个职位的平均工资以及工作年限的数量开始......执行语句为：" + sb.toString());
			List<Map<String, String>> resultLists = mainDao.queryHiveBySql(sb.toString());
			saveDayCitySWCount(resultLists);
			logger.info("执行统计 每天，每个城市，每个职位的平均工资以及工作年限的数量结束......");
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("执行统计 每天，每个城市，每个职位的平均工资以及工作年限的数量失败，失败原因：" + e.getMessage());
		}
	}
	
	/**
	 * Add by linjy on 2016-02-22
	 * 保存统计 每天，每个城市，每个职位的平均工资以及工作年限的数量
	 * @param resultLists
	 */
	private void saveDayCitySWCount(List<Map<String, String>> resultLists){
		try{
			Connection conn = HBaseUtils.getConnection();
			Table table = conn.getTable(TableName.valueOf("SalaryInfoResult".getBytes()));
			logger.info("保存统计 每天，每个城市，每个职位的平均工资以及工作年限的数量开始......统计结果数量：" + resultLists.size());
			List<Put> puts = new ArrayList<Put>();
			for(Map<String, String> map : resultLists){
				String totalCount = map.get("totalcount");
				String city = map.get("city");
				String positionName = map.get("positionname");
				String createDate = map.get("createdate");
				String aveValue = map.get("avevalue");
				String aveWYValue = map.get("avewyvalue");
				//rowKey 增加存储类型标志位	添加平均工资值避免出现rowKey相同情况
				String rowKey = createDate + getPostitionNameSX(positionName) + city + aveValue + "2";
				
				Put put = new Put(rowKey.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "resultCount".getBytes(), totalCount.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "city".getBytes(), city.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "positionName".getBytes(), positionName.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "aveValue".getBytes(), aveValue.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "aveWYValue".getBytes(), aveWYValue.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "createDate".getBytes(), createDate.getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "insertDate".getBytes(), (DateUtils.getDateFormat(new Date(), "yyyyMMdd")).getBytes());
				put.addColumn("ResultInfoFamily".getBytes(), "flag".getBytes(), "2".getBytes());
				
				puts.add(put);
			}
			//插入数据
			table.put(puts);
			//刷新缓冲区
			table.close();
			logger.info("保存统计 每天，每个城市，每个职位的平均工资以及工作年限的数量结果结束......");
		}catch(Exception e){
			e.printStackTrace();
			logger.error("保存统计 每天，每个城市，每个职位的平均工资以及工作年限的数量结果数据到HBase总出错，出错原因：" + e.getMessage());
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
//		StringBuilder sb = new StringBuilder(100);
//		sb.append("select count(positionName) as totalCount,city,positionName,createDate from SalaryInfo ");
//		sb.append(" group by city,positionName,createDate");
//		List<Map<String, String>> resultLists = new MainDaoImpl().queryHiveBySql(sb.toString());
//		System.out.println("统计结果数量：" + resultLists.size());
//		new ScheduleServiceImpl().saveDayCityPCount(resultLists);
		StringBuilder sb = new StringBuilder(100);
		sb.append("select count(positionName) as totalCount,city,positionName,createDate,aveValue from SalaryInfo ");
		sb.append(" group by city,positionName,createDate,aveValue");
		List<Map<String, String>> resultLists = new MainDaoImpl().queryHiveBySql(sb.toString());
		System.out.println("统计结果数量：" + resultLists.size());
		new ScheduleServiceImpl().saveDayCitySCount(resultLists);
	}

	

}