package com.lin.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

public class DaoUtils {
	
	private static Logger logger = LoggerUtils.getLogger("DaoUtils");
	
	/**
	 * Add by linjy on 2015-12-25
	 * @param rsm	结果集元素组
	 * @return
	 * 封装好的 key 字段名	value 值
	 */
	public static List<Map<String,String>> getResultMap(ResultSet rs){
		List<Map<String,String>> resultList = new ArrayList<Map<String,String>>();
		List<String> columName = new ArrayList<String>();
		try{
			ResultSetMetaData rsm = rs.getMetaData();
			for(int i=1;i<=rsm.getColumnCount();i++){
				columName.add(rsm.getColumnName(i));
			}
			while(rs.next()){
				Map<String,String> resuleMap = new HashMap<String, String>();
				for(String name : columName){
					resuleMap.put(name.toLowerCase(), rs.getString(name));
				}
				resultList.add(resuleMap);
			}
		}catch(Exception e){
			e.printStackTrace();
			logger.info("封装多条数据为key 字段名	value 值 出错,出错原因:"+e.getMessage());
		}finally{
			DaoUtils.closeResultSet(rs);
		}
		
		return resultList;
	}
	
	public static void closeConn(Connection conn) {
		if(null != conn){
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("关闭Hive连接失败，失败原因为："+e.getMessage());
			}
		}
	}
	
	public static void closeStatement(Statement  stmt){
		if(null != stmt){
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("关闭Hive Statement失败，失败原因为："+e.getMessage());
			}
		}
	}
	
	public static void closeResultSet(ResultSet  rs) {
		if(null != rs){
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
				logger.error("关闭Hive ResultSet失败，失败原因为："+e.getMessage());
			}
		}
	}
}
