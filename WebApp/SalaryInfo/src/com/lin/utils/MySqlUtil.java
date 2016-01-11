package com.lin.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Add by liny on 2015-12-21
 * @author linjy
 * 连接Mysql数据库工具类
 *
 */
public class MySqlUtil {
	
	private static Logger logger = LoggerUtils.getLogger("MySqlUtil");
	
	public static Connection getConn(){
		ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
		DataSource dataSource = (DataSource) context.getBean("dataSource");
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
			return conn;
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error("获得MySql连接失败，失败原因为："+e.getMessage());
		}
		return conn;
		
	}
	
	
	public static void main(String[] args) throws Exception {
//		Connection conn = getConn();
//		Statement  stmt = null;
//		ResultSet  rs   = null;
//		stmt = conn.createStatement();
//		rs = stmt.executeQuery("SELECT * FROM ctPrInfo");
//		while (rs.next()){
//		    System.out.println( rs.getString(1) );
//	    }
//		rs.close();
//		stmt.close();
//		conn.close();
		String sql = "SELECT * FROM ctPrInfo";
		Connection conn = getConn();
		Statement stmt = conn.createStatement();
		ResultSet  rs = stmt.executeQuery(sql);
		List<Map<String,String>> resultList = DaoUtils.getResultMap(rs);
		System.out.println(resultList);
		DaoUtils.closeStatement(stmt);
		DaoUtils.closeConn(conn);
	}
}
