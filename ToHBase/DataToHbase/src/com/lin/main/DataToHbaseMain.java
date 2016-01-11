package com.lin.main;

import org.apache.log4j.Logger;
import com.lin.server.DataToHBase;
import com.lin.utils.LoggerUtils;

public class DataToHbaseMain {
	
	public static void main(String[] args) throws Exception {
		
		final Logger logger = LoggerUtils.getLogger("DataToHbaseMain");
		if(args.length < 1){
			logger.error("输入参数个数错误，当前参数个数为："+args.length);
			System.exit(0);
		}
		//所需要加载的数据路劲
		final String filePath = args[0];
		
		Thread t1 = new Thread(){
			@Override
			public void run() {
				try{
					logger.info("加载数据("+filePath+")线程开始");
					long startTime = System.currentTimeMillis() / 1000;
					
					DataToHBase dataToHBase = new DataToHBase();
					dataToHBase.loadData(filePath);
					
					long endTime = System.currentTimeMillis() / 1000;
					logger.info("加载数据("+filePath+")线程结束，所花时间为：" + (endTime - startTime) + "秒");
				}catch(Exception e){
					e.printStackTrace();
					logger.error("加载数据("+filePath+")线程出错，出错原因:"+e.getMessage());
				}
				
			}
		};
		
		t1.start();
	}
	
}
