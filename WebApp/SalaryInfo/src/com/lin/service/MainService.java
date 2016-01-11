package com.lin.service;

import java.util.List;
import java.util.Map;

import com.lin.utils.JSONUtil;

import net.sf.json.JSONObject;

public interface MainService {
	public Map<String, List<JSONObject>> getTotalCountByMapUHBase(String date)throws Exception;
	
	public JSONObject getTotalCountByBarUHBase(String startDate,String endDate)throws Exception;
	
	public JSONObject getTotalCountByBarUHive(String startDate,String endDate)throws Exception;
	
	public Map<String,Map<String,String>> getBSGSInfoUHBase(String startDate,String endDate,String familyName) throws Exception;
	
	public Map<String,Map<String,String>> getBSGSInfoUHive(String startDate,String endDate) throws Exception;
	
	public Map<String, List<JSONObject>> getTotalCountByMapUHive(String date) throws Exception;
	
	public Map<String, List<JSONObject>> getTotalCountByMap(String date)throws Exception;
	
	public Map<String,Map<String,String>> getBSGSInfo(String startDate,String endDate,String familyName) throws Exception;
	
	public JSONObject getTotalCountByBar(String startDate,String endDate)throws Exception;
}
