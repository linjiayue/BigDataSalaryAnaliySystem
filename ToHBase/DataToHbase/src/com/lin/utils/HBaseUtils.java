package com.lin.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

/**
 * Add by linjy on 2015-12-15
 * @author ljy
 * HBase 工具类
 */
public class HBaseUtils {
	
	private static Configuration conf = null;
	
	static{
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.zookeeper.quorum", "master");
//		conf.set("hbase.master", "master:60000");
	}
	
	/**
	 * Add by linjy on 2015-12-15
	 * 获得HBase 管理类
	 * @return
	 * @throws Exception
	 */
	public static Connection getConnection() throws Exception{
		Connection conn = ConnectionFactory.createConnection(conf);
		return conn;
	}
	
	/**
	 * Add by linjy on 2015-12-15
	 * 关闭HBase连接
	 * @param conn
	 * @throws IOException
	 */
	public static void closeConn(Connection conn) throws IOException{
		if(null != conn){
			conn.close();
		}
	}
}
