package com.lin.utils;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

/**
 * Add by linjy on 2015-12-15
 * @author ljy
 * HBase 工具类
 */
public class HBaseUtils {
	
	private static Logger logger = LoggerUtils.getLogger("HBaseUtils");
	
	private static Configuration conf = null;
	
	static{
		//加载配置文件的Hive连接信息
		Properties properties = new Properties();
		try {
			properties.load(HBaseUtils.class.getResourceAsStream("/db.properties"));
			conf = HBaseConfiguration.create();
//			conf.set("hbase.zookeeper.property.clientPort", "2181");
//			conf.set("hbase.zookeeper.quorum", "master");
			conf.set("hbase.zookeeper.property.clientPort", properties.getProperty("hbase.zookeeper.property.clientPort"));
			conf.set("hbase.zookeeper.quorum", properties.getProperty("hbase.zookeeper.quorum"));
//			conf.set("hbase.master", "master:60000");
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("配置HBase连接信息出错，出错原因为：" + e.getMessage());
		}
	}
	
	/**
	 * Add by linjy on 2015-12-15
	 * 获得HBase 管理类
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection(){
		
		Connection conn = null;
		try {
			conn = ConnectionFactory.createConnection(conf);
			return conn;
		} catch (IOException e) {
			e.printStackTrace();
			logger.error("获得Hbase连接失败，失败原因为："+e.getMessage());
			
		}
		return conn;
	}
	
	/**
	 * Add by linjy on 2015-12-15
	 * 关闭HBase连接
	 * @param conn
	 * @throws IOException
	 */
	public static void closeConn(Connection conn){
		if(null != conn){
			try {
				conn.close();
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("关闭Hbase连接失败，失败原因为："+e.getMessage());
			}
		}
	}
}
