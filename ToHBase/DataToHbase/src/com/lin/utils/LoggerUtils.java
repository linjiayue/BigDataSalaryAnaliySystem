package com.lin.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.LoggerFactory;


public class LoggerUtils {
	
	private static Logger logger = null;
	
	public static Logger getLogger(String className){
		logger = Logger.getLogger(className);
		try{
			PropertyConfigurator.configure(LoggerUtils.class.getResourceAsStream("/log4j.properties"));
		}catch(Exception e){
			e.printStackTrace();
			logger.error("加载日志配置文件出错...");
		}
		return logger;
	}
}
