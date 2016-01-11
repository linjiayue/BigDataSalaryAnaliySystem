package com.lin.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

public class HiveUtils {
	
	private static Logger logger = LoggerUtils.getLogger("HiveUtils");
	
//	private static final String driverName = "org.apache.hive.jdbc.HiveDriver";
//	private static final String dirverURL = "jdbc:hive2://192.168.192.12:10000/default";
//	
//	private static final String userName = "hive";
//	private static final String userPassword = "hive";
	
	private static String driverName = null;
	private static String dirverURL = null;
	private static String userName = null;
	private static String userPassword = null;
	static{
		try {
			//加载配置文件的Hive连接信息
			Properties properties = new Properties();
			properties.load(HiveUtils.class.getResourceAsStream("/db.properties"));
			driverName = properties.getProperty("hdriverName");
			dirverURL = properties.getProperty("hdirverURL");
			userName = properties.getProperty("huserName");
			userPassword = properties.getProperty("huserPassword");
			
			Class.forName(driverName);
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("初始化连接配置出错，错误原因：" + e.getMessage());
		}
	}
	
	/**
	 * Add by linjy on 2015-12-25
	 * 获得Hive JDBC 连接
	 * @return
	 */
	public static Connection getConn(){
		Connection conn = null;
		try {
			conn = DriverManager.getConnection(dirverURL, userName, userPassword);
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("获取Hive连接失败，失败原因为：" + e.getMessage());
		}
		return conn;
	}
	
	public List<Map<String,String>> queryBySql(String sql) throws Exception{
		List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
		
//		conn = DriverManager.getConnection(dirverURL, userName, userPassword);
		Connection conn = getConn();
		Statement st = conn.createStatement();
		List<String> columName = new ArrayList<String>();
		ResultSet rs = st.executeQuery(sql);
		ResultSetMetaData rsm = rs.getMetaData();
		for(int i=1;i<=rsm.getColumnCount();i++){
			columName.add(rsm.getColumnName(i));
		}
		while(rs.next()){
			Map<String,String> resuleMap = new HashMap<String, String>();
			for(String name : columName){
				resuleMap.put(name, rs.getString(name));
			}
			resultList.add(resuleMap);
		}
		rs.close();
		st.close();
		conn.close();
		return resultList;
	}
		
	public static void main(String[] args) throws Exception {
		String sql = "select * from SalaryInfo";
		List<Map<String,String>> resultList = new HiveUtils().queryBySql(sql);
		for(Map<String,String> result : resultList){
			System.out.println(result);
		}
	}
}
