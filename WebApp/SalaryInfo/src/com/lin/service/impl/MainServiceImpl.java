package com.lin.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lin.dao.MainDao;
import com.lin.service.MainService;
import com.lin.utils.LoggerUtils;

@Service
public class MainServiceImpl implements MainService {
	@Autowired
	private MainDao mainDao;
	private Logger logger = LoggerUtils.getLogger("MainServiceImpl");
	
	/**
	 * Add by linjy 2015-12-17
	 * @param date	所查询年份，ex 2015
	 * 返回职位需求地图表数据
	 * @throws Exception 
	 * 
	 */
	@Override
	public Map<String, List<JSONObject>> getTotalCountByMapUHBase(String date) throws Exception {
		
		String startDate = date + "0101";
		String endDate = date + "1231";
		logger.info("返回职位需求地图表数据处理开始.....");
		List<Map<String, String>> resultList = mainDao.scanByDateFamily(startDate, endDate, "PositionInfoFamily");
		
		return getTotalCountByMap(resultList);
	}
	
	/**
	 * Add by linjy 2015-12-21
	 * @param positionName	职位名称
	 * @return
	 * 判断职位名称中含有几个关键词
	 */
	private List<String> getPositionKey(String positionName){
		
		List<String> pList = new ArrayList<String>();
		if(positionName.toUpperCase().contains("Hadoop".toUpperCase())){
			pList.add("Hadoop");
		}
		if(positionName.toUpperCase().contains("Spark".toUpperCase())){
			pList.add("Spark");
		}
		if(positionName.contains("数据挖掘")){
			pList.add("DM");
		}
		if(positionName.contains("数据分析")){
			pList.add("DA");
		}
		return pList;
	}
	/**
	 * Add by linjy 2016-01-06
	 * @param positionName	职位名称
	 * @return
	 * 返回职位名称缩写
	 */
	private String getPositionKeyStr(String positionName){
		
		if(positionName.toUpperCase().contains("Hadoop".toUpperCase())){
			return "Hadoop";
		}
		if(positionName.toUpperCase().contains("Spark".toUpperCase())){
			return "Spark";
		}
		if(positionName.contains("数据挖掘")){
			return "DM";
		}
		if(positionName.contains("数据分析")){
			return "DA";
		}
		return "";
	}
	
	/**
	 * Add by linjy 2015-12-18
	 * @param startDate	开始时间
	 * @param endDate	结束时间
	 * 返回总数柱状图数据
	 */
	@Override
	public JSONObject getTotalCountByBarUHBase(String startDate,String endDate) throws Exception {
		logger.info("返回总数柱状图数据处理开始.....");
		List<Map<String, String>> resultList = mainDao.scanByDateFamily(startDate, endDate, "PositionInfoFamily");
		return getTotalCountByBar(resultList);
	}
	/**
	 * Add by linjy 2015-12-21
	 * @param startDate	开始时间
	 * @param endDate	结束时间
	 * @param familyName	簇名
	 * 返回总数柱状图数据
	 */
	@Override
	public Map<String,Map<String,String>> getBSGSInfoUHBase(String startDate,String endDate, String familyName) throws Exception {
		logger.info("总数柱状图数据处理开始....");
		List<Map<String, String>> resultList = mainDao.getBSGSInfoForHBase(startDate, endDate, familyName);
		return getBSGSInfo(resultList);
	}
	/**
	 * Add by linjy 2015-12-28
	 * @param startDate	开始时间
	 * @param endDate	结束时间
	 * 返回总数柱状图数据
	 */
	@Override
	public Map<String,Map<String,String>> getBSGSInfoUHive(String startDate,String endDate) throws Exception {
		logger.info("总数柱状图数据处理开始....");
		StringBuilder sql = new StringBuilder(50);
		sql.append("select positionname,city from SalaryInfo ");
		sql.append(" where city in('北京','上海','广州','深圳') and to_date(createtime) between to_date('"+startDate+"') ");
		sql.append(" and to_date('"+endDate+"') ");
		
		List<Map<String, String>> resultList = mainDao.queryHiveBySql(sql.toString());
		return getBSGSInfo(resultList);
	}
	
	/**
	 * Add by linjy 2015-12-21
	 * @param cityName	城市名
	 * @return
	 * 返回北上广深对应key
	 */
	private String getBSGSKey(String cityName){
		if("北京".equals(cityName)){
			return "BJ";
		}else if("上海".equals(cityName)){
			return "SH";
		}else if("广州".equals(cityName)){
			return "GZ";
		}else if("深圳".equals(cityName)){
			return "SZ";
		}
		
		return null;
	}
	/**
	 * Add by linjy 2015-12-21
	 * @param resultList	查询的具有职位名称以及对应城市的数据
	 * @return
	 * 返回封装好的城市与职位数据，对应关系为，key 职位    value(key 城市  value 数量)
	 */
	private Map<String,Map<String,String>> getCPMap(List<Map<String, String>> resultList){
		//获得省份城市对应Map
		Map<String,String> cpMap = mainDao.getCityProMap();
		Map<String,Map<String,String>> resultMap = new HashMap<String, Map<String,String>>();
		for(Map<String,String> map : resultList){
			
			List<String> pList = getPositionKey(map.get("positionName"));
			String city = (null ==cpMap.get(map.get("city"))) ? map.get("city") : cpMap.get(map.get("city"));
			for(String pKey : pList){
				Map<String,String> tmpMap = (null == resultMap.get(pKey)) ? (new HashMap<String, String>()) : resultMap.get(pKey) ;
				int count = Integer.parseInt((null == tmpMap.get(city))? "0" : tmpMap.get(city));
				count++;
				tmpMap.put(city, count + "");
				
				resultMap.put(pKey, tmpMap);
			}
			
		}
		return resultMap;
	}
	/**
	 * Add by linjy 2015-12-28
	 * @param date	所查询年份，ex 2015
	 * 返回地图展示所需要的数据，数据来源为Hive
	 */
	@Override
	public Map<String, List<JSONObject>> getTotalCountByMapUHive(String date)
			throws Exception {
		String startDate = date + "-01-01";
		String endDate = date + "-12-31";
		StringBuilder sql = new StringBuilder(50);
		sql.append("select positionname,city from SalaryInfo ");
		sql.append(" where to_date(createtime) between to_date('"+startDate+"') ");
		sql.append(" and to_date('"+endDate+"') ");
		
		List<Map<String, String>> resultList = mainDao.queryHiveBySql(sql.toString());
		return getTotalCountByMap(resultList);
	}
	
	/**
	 * Add by linjy on 2015-12-28
	 * @param resultList	封装好的数据
	 * @return
	 * 返回Map需要的数据
	 */
	private Map<String, List<JSONObject>> getTotalCountByMap(List<Map<String, String>> resultList){
		//获得省份城市对应Map
		Map<String,String> cpMap = mainDao.getCityProMap();
		Map<String,Map<String,String>> resultMap = new HashMap<String, Map<String,String>>();
		for(Map<String,String> map : resultList){
			
			List<String> pList = getPositionKey(map.get("positionname"));
			String city = (null ==cpMap.get(map.get("city"))) ? map.get("city") : cpMap.get(map.get("city"));
			for(String pKey : pList){
				Map<String,String> tmpMap = (null == resultMap.get(pKey)) ? (new HashMap<String, String>()) : resultMap.get(pKey) ;
				int count = Integer.parseInt((null == tmpMap.get(city))? "0" : tmpMap.get(city));
				count++;
				tmpMap.put(city, count + "");
				
				resultMap.put(pKey, tmpMap);
			}
			
		}
		
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		for(Entry<String,Map<String,String>> entry : resultMap.entrySet()){
			
			List<JSONObject> objects = new ArrayList<JSONObject>();
			for(Entry<String,String> et : (entry.getValue()).entrySet()){
				JSONObject object = new JSONObject();
				object.put("name", et.getKey());
				object.put("value", Integer.parseInt(et.getValue()));
				objects.add(object);
			}
			
			map.put(entry.getKey(), objects);
		}
		return map;
	}
	
	/**
	 * Add by linjy 2015-12-28
	 * @param resultList	封装好的数据
	 * 返回总数柱状图数据
	 */
	private Map<String,Map<String,String>> getBSGSInfo(List<Map<String, String>> resultList) throws Exception {
		//获得省份城市对应Map
		Map<String,String> cpMap = mainDao.getCityProMap();
		Map<String,Map<String,String>> resultMap = new HashMap<String, Map<String,String>>();
		for(Map<String,String> map : resultList){
			
			List<String> pList = getPositionKey(map.get("positionname"));
			String city = map.get("city");
			for(String pKey : pList){
				Map<String,String> tmpMap = (null == resultMap.get(pKey)) ? (new HashMap<String, String>()) : resultMap.get(pKey) ;
				int count = Integer.parseInt((null == tmpMap.get(city))? "0" : tmpMap.get(city));
				count++;
				tmpMap.put(city, count + "");
				
				resultMap.put(pKey, tmpMap);
			}
			
		}
		
		Map<String,Map<String,String>> map = new HashMap<String, Map<String,String>>();
		for(Entry<String,Map<String,String>> entry : resultMap.entrySet()){
			Map<String,String> tmpMap = new HashMap<String, String>();
			for(Entry<String,String> et : (entry.getValue()).entrySet()){
				tmpMap.put(getBSGSKey(et.getKey()), et.getValue());
			}
			map.put(entry.getKey(), tmpMap);
		}
		logger.info("总数柱状图数据处理结束....");
		return map;
	}
	
	/**
	 * Add by linjy 2015-12-28
	 * @param resultList	封装好的数据
	 * @return	返回柱状图数据
	 */
	private JSONObject getTotalCountByBar(List<Map<String, String>> resultList){
		Map<String,Integer> resultMap = new HashMap<String, Integer>();
		for(Map<String, String> map : resultList){
			
//			String pKey = getPositionKey(map.get("positionName"));
			List<String> pList = getPositionKey(map.get("positionname"));
			for(String pKey : pList){
				int count = ((null == resultMap.get(pKey)) ? 0 : resultMap.get(pKey));
				count ++;
				resultMap.put(pKey, count);
			}
		}
		JSONObject object = new JSONObject();
		for(Entry<String,Integer> entry : resultMap.entrySet()){
			object.put(entry.getKey(), entry.getValue());
		}
		logger.info("返回总数柱状图数据处理结束.....");
		return object;
	}
	/**
	 * Add by linjy 2015-12-28
	 * @param startDate	开始时间
	 * @param endDate	结束时间
	 * 使用Hive数据源返回柱状图数据
	 */
	@Override
	public JSONObject getTotalCountByBarUHive(String startDate, String endDate)
			throws Exception {
		StringBuilder sql = new StringBuilder(50);
		sql.append("select positionname,city from SalaryInfo ");
		sql.append(" where to_date(createtime) between to_date('"+startDate+"') ");
		sql.append(" and to_date('"+endDate+"') ");
		List<Map<String, String>> resultList = mainDao.queryHiveBySql(sql.toString());
		return getTotalCountByBar(resultList);
	}
	
	/**
	 * Add by linjy 2016-01-06
	 * @param date 查询年份
	 * 返回所查询的年份的统计数量
	 */
	@Override
	public Map<String, List<JSONObject>> getTotalCountByMap(String date)
			throws Exception {
		String startDate = date + "0101";
		String endDate = date + "1231";
		
		List<Map<String, String>> resultList = mainDao.scanResultByDate(startDate, endDate, "ResultInfoFamily","0");
		//获得省份城市对应Map
		Map<String,String> cpMap = mainDao.getCityProMap();
		Map<String,Map<String,String>> resultMap = new HashMap<String, Map<String,String>>();
		for(Map<String,String> map : resultList){
			
			String positionName = getPositionKeyStr(map.get("positionname"));
			String city = (null ==cpMap.get(map.get("city"))) ? map.get("city") : cpMap.get(map.get("city"));
			Map<String,String> tmpMap = (null == resultMap.get(positionName)) ? (new HashMap<String, String>()) : resultMap.get(positionName) ;
			int count = Integer.parseInt((null == tmpMap.get(city))? "0" : tmpMap.get(city));
			count = count + Integer.parseInt(map.get("resultcount"));
			tmpMap.put(city, count + "");
			
			resultMap.put(positionName, tmpMap);
			
		}
		
		Map<String, List<JSONObject>> map = new HashMap<String, List<JSONObject>>();
		for(Entry<String,Map<String,String>> entry : resultMap.entrySet()){
			
			List<JSONObject> objects = new ArrayList<JSONObject>();
			for(Entry<String,String> et : (entry.getValue()).entrySet()){
				JSONObject object = new JSONObject();
				object.put("name", et.getKey());
				object.put("value", Integer.parseInt(et.getValue()));
				objects.add(object);
			}
			
			map.put(entry.getKey(), objects);
		}
		return map;
	}
	
	/**
	 * Add by linjy 2016-01-07
	 * @param startDate	开始时间
	 * @param endDate	结束时间
	 * @param familyName	列簇名
	 * 返回统计结果中的北上广堆积图内容
	 */
	@Override
	public Map<String, Map<String, String>> getBSGSInfo(String startDate,
			String endDate, String familyName) throws Exception {
		List<Map<String, String>> resultList = mainDao.getBSGSInfo(startDate, endDate, familyName);
		//获得省份城市对应Map
		Map<String,String> cpMap = mainDao.getCityProMap();
		Map<String,Map<String,String>> resultMap = new HashMap<String, Map<String,String>>();
		for(Map<String,String> map : resultList){
			
			String positionName = getPositionKeyStr(map.get("positionname"));
			String city = map.get("city");
			Map<String,String> tmpMap = (null == resultMap.get(positionName)) ? (new HashMap<String, String>()) : resultMap.get(positionName) ;
			int count = Integer.parseInt((null == tmpMap.get(city))? "0" : tmpMap.get(city));
			count = count + Integer.parseInt(map.get("resultcount"));
			tmpMap.put(city, count + "");
			
			resultMap.put(positionName, tmpMap);
			
		}
		
		Map<String,Map<String,String>> map = new HashMap<String, Map<String,String>>();
		for(Entry<String,Map<String,String>> entry : resultMap.entrySet()){
			Map<String,String> tmpMap = new HashMap<String, String>();
			for(Entry<String,String> et : (entry.getValue()).entrySet()){
				tmpMap.put(getBSGSKey(et.getKey()), et.getValue());
			}
			map.put(entry.getKey(), tmpMap);
		}
		logger.info("总数柱状图数据处理结束....");
		return map;
	}
	
	/**
	 * Add by linjy 2015-01-07
	 * @param startDate	开始时间
	 * @param endDate	结束时间
	 * 返回统计结果中总数柱状图数据
	 */
	@Override
	public JSONObject getTotalCountByBar(String startDate,String endDate) throws Exception {
		logger.info("返回总数柱状图数据处理开始.....");
		List<Map<String, String>> resultList = mainDao.scanResultByDate(startDate, endDate, "ResultInfoFamily","0");
		Map<String,Integer> resultMap = new HashMap<String, Integer>();
		for(Map<String, String> map : resultList){
			
			String positionName = getPositionKeyStr(map.get("positionname"));
			int count = ((null == resultMap.get(positionName)) ? 0 : resultMap.get(positionName));
			count = count + Integer.parseInt(map.get("resultcount"));
			resultMap.put(positionName, count);
		}
		JSONObject object = new JSONObject();
		for(Entry<String,Integer> entry : resultMap.entrySet()){
			object.put(entry.getKey(), entry.getValue());
		}
		logger.info("返回总数柱状图数据处理结束.....");
		return object;
	}
}
