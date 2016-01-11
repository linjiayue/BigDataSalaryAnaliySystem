package com.lin.dao;

import java.util.List;
import java.util.Map;

public interface MainDao {
	
	public List<Map<String, String>> scanByDateFamily(String startDate,String endDate,String familyName) throws Exception;
	
	public Map<String,String> getCityProMap();
	
	public List<Map<String, String>> getBSGSInfoForHBase(String startDate,String endDate,String familyName) throws Exception;
	
	public List<Map<String, String>> queryHiveBySql(String sql) throws Exception;
	
	public void executeSqlForHive(String sql) throws Exception;
	
	public List<Map<String, String>> scanResultByDate(String startDate,
			String endDate, String familyName,String flag) throws Exception;
	
	public List<Map<String, String>> getBSGSInfo(String startDate,String endDate,String familyName) throws Exception;
	
	public boolean tableIsTmpty(String tableName) throws Exception;
}
