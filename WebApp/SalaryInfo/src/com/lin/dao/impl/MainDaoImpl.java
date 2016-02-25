package com.lin.dao.impl;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Repository;

import com.lin.dao.MainDao;
import com.lin.utils.DaoUtils;
import com.lin.utils.HBaseUtils;
import com.lin.utils.HiveUtils;
import com.lin.utils.LoggerUtils;
import com.lin.utils.MySqlUtil;

@Repository
public class MainDaoImpl implements MainDao {
	
	private Logger logger = LoggerUtils.getLogger("MainDaoImpl");
	/**
	 * Add by linjy on 2015-12-17
	 * @param startDate	扫面开始时间段	格式为：yyyyMMdd
	 * @param endDate	扫面结束时间段
	 * @param familyName	查询指定的列簇名
	 * @return
	 * @throws Exception 
	 */
	@Override
	public List<Map<String, String>> scanByDateFamily(String startDate,
			String endDate, String familyName) throws Exception {
		logger.info("读取HBase数据开始.....");
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("SalaryInfo".getBytes()));
		Scan scan = new Scan();
		//指定查询的族
		if(StringUtils.isNotBlank(familyName)){
			scan.addFamily(familyName.getBytes());
		}
		scan.setStartRow(startDate.getBytes());
		scan.setStopRow(endDate.getBytes());
		
		List<Map<String, String>> resultList = new ArrayList<Map<String,String>>();
		ResultScanner rss = table.getScanner(scan);
		logger.info("封装HBase数据开始.....");
		for(Result rs : rss){
			Map<String, String> map = new HashMap<String, String>();
			map.put("rowKey", new String(rs.getRow()));
			for(Cell cell : rs.rawCells()){
				map.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(),new String(CellUtil.cloneValue(cell)));
			}
			resultList.add(map);
		}
		logger.info("封装HBase数据结束.....");
		table.close();
		HBaseUtils.closeConn(conn);
		logger.info("读取HBase数据结束.....");
		return resultList;
	}
	
	/**
	 * Add by linjy on 2015-12-21
	 * 获得城市与省份对应关系，其中对城市进行出去最后的'市'字符串的处理
	 */
	@Override
	public Map<String, String> getCityProMap() {
		Map<String,String> map = new HashMap<String, String>();
		java.sql.Connection conn = MySqlUtil.getConn();
		Statement st = null;
		ResultSet rs = null;
		if(null != conn){
			try {
				st = conn.createStatement();
				String sql = "SELECT cityName,provinName FROM ctPrInfo";
				rs = st.executeQuery(sql);
				while (rs.next()){
					String cityName = rs.getString(1);
					cityName = cityName.substring(0, cityName.length()-1);
					String provinName = rs.getString(2);
					map.put(cityName, provinName);
			    }
				return map;
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("获得城市与省份对应关系失败，失败原因："+e.getMessage());
			}finally{
				DaoUtils.closeResultSet(rs);
				DaoUtils.closeStatement(st);
				DaoUtils.closeConn(conn);
			}
		}
		return null;
	}
	
	/**
	 * Add by linjy on 2015-12-21
	 * 返回北上广深分别对应的职位需求量
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Override
	public List<Map<String, String>> getBSGSInfoForHBase(String startDate,String endDate, String familyName) throws Exception {
		
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("SalaryInfo".getBytes()));
		Scan scan = new Scan();
//		scan.addColumn("PositionInfoFamily".getBytes(), "positionName".getBytes());
//		scan.addColumn("PositionInfoFamily".getBytes(), "city".getBytes());
		//指定查询的族
		if(StringUtils.isNotBlank(familyName)){
			scan.addFamily(familyName.getBytes());
		}
		scan.setStartRow(startDate.getBytes());
		scan.setStopRow(endDate.getBytes());
		//添加过滤，只查询 北京 上海 深圳 广州的信息
//		Filter filter = new SingleColumnValueFilter("PositionInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL, new RegexStringComparator("北京|上海|深圳|广州"));
		
		//FilterList.Operator.MUST_PASS_ONE 或
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		filterList.addFilter(new SingleColumnValueFilter("PositionInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "北京".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("PositionInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "上海".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("PositionInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "深圳".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("PositionInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "广州".getBytes()));
		scan.setFilter(filterList);
		
		List<Map<String, String>> resultList = new ArrayList<Map<String,String>>();
		ResultScanner rss = table.getScanner(scan);
		logger.info("封装HBase数据开始.....");
		for(Result rs : rss){
			Map<String, String> map = new HashMap<String, String>();
			map.put("rowKey", new String(rs.getRow()));
			for(Cell cell : rs.rawCells()){
				map.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(),new String(CellUtil.cloneValue(cell)));
			}
			resultList.add(map);
		}
		table.close();
		HBaseUtils.closeConn(conn);
		return resultList;
	}

	/**
	 * Add by linjy on 2015-12-28
	 * @param sql 需要执行的查询语句
	 * 返回查询结果，并封装到Map中
	 */
	@Override
	public List<Map<String, String>> queryHiveBySql(String sql)
			throws Exception {
		logger.info("读取Hive数据，执行语句为：" + sql);
		java.sql.Connection conn = HiveUtils.getConn();
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery(sql);
		List<Map<String, String>> resultList = DaoUtils.getResultMap(rs);
		DaoUtils.closeStatement(st);
		DaoUtils.closeConn(conn);
		return resultList;
	}
	
	/**
	 * Add by linjy on 2016-01-06
	 * 执行不返回结果的Sql语句
	 */
	@Override
	public void executeSqlForHive(String sql) throws Exception {
		logger.info("读取Hive数据，执行语句为：" + sql);
		java.sql.Connection conn = HiveUtils.getConn();
		Statement st = conn.createStatement();
		st.execute(sql);
		DaoUtils.closeStatement(st);
		DaoUtils.closeConn(conn);
	}
	/**
	 * Add by linjy on 2016-01-06
	 * @param dateTime	查询时间
	 * @param familyName	簇名称
	 * @param flag	标志	0	存储针对数量分析的数据	1	存储针对薪资分析的数据	
	 * 扫描指定日期
	 */
	@Override
	public List<Map<String, String>> scanResultByDate(String startDate,
			String endDate, String familyName,String flag) throws Exception {
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("SalaryInfoResult".getBytes()));
		Scan scan = new Scan();
		scan.setStartRow(startDate.getBytes());
		scan.setStopRow(endDate.getBytes());
		scan.addFamily(familyName.getBytes());
		
		Filter filter = new SingleColumnValueFilter("ResultInfoFamily".getBytes(),"flag".getBytes(),CompareOp.EQUAL,flag.getBytes());
		scan.setFilter(filter);
		List<Map<String, String>> resultList = new ArrayList<Map<String,String>>();
		ResultScanner rss = table.getScanner(scan);
		for(Result rs : rss){
			Map<String, String> map = new HashMap<String, String>();
			map.put("rowKey", new String(rs.getRow()));
			for(Cell cell : rs.rawCells()){
				map.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(),new String(CellUtil.cloneValue(cell)));
			}
			resultList.add(map);
		}
		table.close();
		HBaseUtils.closeConn(conn);
		return resultList;
	}
	
	/**
	 * Add by linjy on 2016-01-07
	 * 返回北上广深分别对应的职位需求量
	 * @throws Exception 
	 * @throws IllegalArgumentException 
	 */
	@Override
	public List<Map<String, String>> getBSGSInfo(String startDate,String endDate, String familyName,String flag) throws Exception {
		
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("SalaryInfoResult".getBytes()));
		Scan scan = new Scan();
		//指定查询的族
		if(StringUtils.isNotBlank(familyName)){
			scan.addFamily(familyName.getBytes());
		}
		scan.setStartRow(startDate.getBytes());
		scan.setStopRow(endDate.getBytes());
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
		filterList.addFilter(new SingleColumnValueFilter("ResultInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "北京".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("ResultInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "上海".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("ResultInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "深圳".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("ResultInfoFamily".getBytes(), "city".getBytes(), CompareOp.EQUAL,  "广州".getBytes()));
		filterList.addFilter(new SingleColumnValueFilter("ResultInfoFamily".getBytes(),"flag".getBytes(),CompareOp.EQUAL,flag.getBytes()));
		scan.setFilter(filterList);
		
		List<Map<String, String>> resultList = new ArrayList<Map<String,String>>();
		ResultScanner rss = table.getScanner(scan);
		logger.info("封装HBase数据开始.....");
		for(Result rs : rss){
			Map<String, String> map = new HashMap<String, String>();
			map.put("rowKey", new String(rs.getRow()));
			for(Cell cell : rs.rawCells()){
				map.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(),new String(CellUtil.cloneValue(cell)));
			}
			resultList.add(map);
		}
		table.close();
		HBaseUtils.closeConn(conn);
		return resultList;
	}
	
	/**
	 * Add by linjy on 2016-01-07
	 * @param tableName	表名
	 * 判断表是否为空，空 返回 true 不空返回false
	 */
	@Override
	public boolean tableIsTmpty(String tableName) throws Exception {
		boolean falg = false;
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("T".getBytes()));
		Scan scan = new Scan();
		ResultScanner rss = table.getScanner(scan);
		Result rs = rss.next();
		if(null == rs){
			falg = true;
		}
		table.close();
		HBaseUtils.closeConn(conn);
		return falg;
	}
	/**
	 * Add by linjy on 2016-02-19
	 * @param tableName		表名
	 * @param familyName	簇名
	 * @param flag			区分标志位
	 * @return
	 * @throws Exception
	 * 判断指定标志下是否有内容
	 */
	public boolean tableIsTmptyByFlag(String tableName,String familyName,String flag) throws Exception {
		boolean falg = false;
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("T".getBytes()));
		Scan scan = new Scan();
		Filter filter = new SingleColumnValueFilter(familyName.getBytes(),"flag".getBytes(),CompareOp.EQUAL,flag.getBytes());
		scan.setFilter(filter);
		ResultScanner rss = table.getScanner(scan);
		Result rs = rss.next();
		if(null == rs){
			falg = true;
		}
		table.close();
		HBaseUtils.closeConn(conn);
		return falg;
	}
	
	public static void main(String[] args) throws Exception, IOException {
		Connection conn = HBaseUtils.getConnection();
		Table table = conn.getTable(TableName.valueOf("SalaryInfoResult".getBytes()));
		Scan scan = new Scan();
//		scan.addFamily("ResultInfoFamily".getBytes());
		Filter filter = new SingleColumnValueFilter("ResultInfoFamily".getBytes(),"flag".getBytes(),CompareOp.EQUAL,"1".getBytes());
		scan.setFilter(filter);
		ResultScanner rss = table.getScanner(scan);
		Result rs = rss.next();
		if(null == rs){
			System.out.println("为空");
		}else{
			System.out.println("不为空");
		}
//		for(Result rs : rss){
////			Map<String, String> map = new HashMap<String, String>();
////			map.put("rowKey", new String(rs.getRow()));
////			for(Cell cell : rs.rawCells()){
////				map.put(new String(CellUtil.cloneQualifier(cell)).toLowerCase(),new String(CellUtil.cloneValue(cell)));
////			}
////			System.out.println(map.toString());
//			System.out.println(rs.isEmpty());
//		}
		table.close();
		HBaseUtils.closeConn(conn);
	}

	
}
