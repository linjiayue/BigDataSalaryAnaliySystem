package com.lin.controller;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.lin.service.MainService;
import com.lin.utils.DateUtils;
import com.lin.utils.LoggerUtils;

/**
 * Add by linjy on 2015-12-17
 * @author linjy
 * 职位分析控制类
 *
 */
@Controller
@RequestMapping("/mainController")
public class MainController {
	
	@Autowired
	private MainService mainService;
	
	private Logger logger = LoggerUtils.getLogger("MainController");
	
	/**
	 * Add by linjy on 2016-01-06
	 * 跳转到首页
	 * @return
	 */
	@RequestMapping("/index.action")
	public String index(){
		return "index";
	}
	
	/**
	 * Add by linjy on 2015-12-17
	 * 跳转到首页
	 * @return
	 */
	@RequestMapping("/indexForHBase.action")
	public String indexForHBase(){
		return "indexForHBase";
	}
	/**
	 * Add by linjy on 2015-12-17
	 * 跳转到首页
	 * @return
	 */
	@RequestMapping("/goIndexForHive.action")
	public String goIndexForHive(){
		return "indexForHive";
	}
	/**
	 * Add by linjy on 2015-12-17
	 * 获取地图数据
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTotalCountByMapUHBase.action")
	public Map<String, List<JSONObject>> getTotalCountByMapUHBase(String startDate){
		
//		String date = "2015";
		String date = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") : startDate;
		Map<String, List<JSONObject>> map;
		try {
			logger.info("获取地图数据开始.....");
			map = mainService.getTotalCountByMapUHBase(date);
			logger.info("获取地图数据结束.....");
			return map;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取总数量出错，出错原因为：" + e.getMessage());
		}
		return null;
	}
	/**
	 * Add by linjy on 2015-12-28
	 * 获取地图数据,数据源使用Hive
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTotalCountByMapUHive.action")
	public Map<String, List<JSONObject>> getTotalCountByMapUHive(String startDate){
		
		String date = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") : startDate;
		Map<String, List<JSONObject>> map;
		try {
			logger.info("获取地图数据开始.....");
			map = mainService.getTotalCountByMapUHive(date);
			logger.info("获取地图数据结束.....");
			return map;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取总数量出错，出错原因为：" + e.getMessage());
		}
		return null;
	}
	/**
	 * Add by linjy on 2015-12-21
	 * @return
	 * 返回北上广深职位需求量折线图数据
	 */
	@ResponseBody
	@RequestMapping("/getBSGSInfoUHBase.action")
	public Map<String,Map<String,String>> getBSGSInfoUHBase(String startDate,String endDate){
		try{
			logger.info("返回北上广深职位需求量折线图数据处理开始....");
			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "0101" : formatDate(startDate);
			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "1231" : formatDate(endDate);
			Map<String,Map<String,String>> map = mainService.getBSGSInfoUHBase(startDate, endDate, "PositionInfoFamily");
			logger.info("返回北上广深职位需求量折线图数据处理结束....");
			return map;
		}catch(Exception e){
			e.printStackTrace();
			logger.error("返回北上广深职位需求量折线图数据出错，出错原因："+e.getMessage());
		}
		return null;
	}
	/**
	 * Add by linjy on 2015-12-28
	 * @return
	 * 返回北上广深职位需求量折线图数据,使用Hive数据源
	 */
	@ResponseBody
	@RequestMapping("/getBSGSInfoUHive.action")
	public Map<String,Map<String,String>> getBSGSInfoUHive(String startDate,String endDate){
		try{
			logger.info("返回北上广深职位需求量折线图数据处理开始....");
			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "-01-01" : startDate.trim();
			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "-12-31" : endDate.trim();
//			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "0101" : formatDate(startDate);
//			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "1231" : formatDate(endDate);
			Map<String,Map<String,String>> map = mainService.getBSGSInfoUHive(startDate, endDate);
			logger.info("返回北上广深职位需求量折线图数据处理结束....");
			return map;
		}catch(Exception e){
			e.printStackTrace();
			logger.error("返回北上广深职位需求量折线图数据出错，出错原因："+e.getMessage());
		}
		return null;
	}
	
	/**
	 * Add by linjy on 2015-12-18
	 * 获取职位需求总量
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTotalCountByBarUHBase.action")
	public JSONObject getTotalCountByBarUHBase(String startDate,String endDate){
		try{
			logger.info("获取职位需求总量数据处理开始....");
			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "0101" : formatDate(startDate);
			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "1231" : formatDate(endDate);
			
			JSONObject object = mainService.getTotalCountByBarUHBase(startDate, endDate);
			logger.info("获取职位需求总量数据处理结束....");
			return object;
		}catch(Exception e){
			e.printStackTrace();
			logger.error("获取职位需求总量数据出错，出错原因："+e.getMessage());
		}
		return null;
	}
	/**
	 * Add by linjy on 2015-12-28
	 * 获取职位需求总量,使用Hive数据源
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTotalCountByBarUHive.action")
	public JSONObject getTotalCountByBarUHive(String startDate,String endDate){
		try{
			logger.info("获取职位需求总量数据处理开始....");
			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "-01-01" : startDate.trim();
			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "-12-31" : endDate.trim();
//			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "0101" : formatDate(startDate);
//			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "1231" : formatDate(endDate);
			
			JSONObject object = mainService.getTotalCountByBarUHive(startDate, endDate);
			logger.info("获取职位需求总量数据处理结束....");
			return object;
		}catch(Exception e){
			e.printStackTrace();
			logger.error("获取职位需求总量数据出错，出错原因："+e.getMessage());
		}
		return null;
	}
	
	/**
	 * Add by linjy on 2015-12-23
	 * @param date
	 * @return
	 * 将 2015-12-21 这样的格式转化为 20151221
	 */
	private String formatDate(String date){
		String[] dates = date.split("-");
		date = dates[0] + dates[1] + dates[2];
		return date;
	}
	
	
	/**
	 * Add by linjy on 2016-01-06
	 * 获取地图数据,使用查询结果表
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTotalCountByMap.action")
	public Map<String, List<JSONObject>> getTotalCountByMap(String startDate){
		
		String date = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") : startDate;
		Map<String, List<JSONObject>> map;
		try {
			logger.info("获取地图数据开始.....");
			map = mainService.getTotalCountByMap(date);
			logger.info("获取地图数据结束.....");
			return map;
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取总数量出错，出错原因为：" + e.getMessage());
		}
		return null;
	}
	
	/**
	 * Add by linjy on 2015-12-21
	 * @return
	 * 返回北上广深职位需求量折线图数据
	 */
	@ResponseBody
	@RequestMapping("/getBSGSInfo.action")
	public Map<String,Map<String,String>> getBSGSInfo(String startDate,String endDate){
		try{
			logger.info("返回北上广深职位需求量折线图数据处理开始....");
			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "0101" : formatDate(startDate);
			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "1231" : formatDate(endDate);
			Map<String,Map<String,String>> map = mainService.getBSGSInfo(startDate, endDate, "ResultInfoFamily");
			logger.info("返回北上广深职位需求量折线图数据处理结束....");
			return map;
		}catch(Exception e){
			e.printStackTrace();
			logger.error("返回北上广深职位需求量折线图数据出错，出错原因："+e.getMessage());
		}
		return null;
	}
	
	/**
	 * Add by linjy on 2016-01-07
	 * 获取职位需求总量
	 * @return
	 */
	@ResponseBody
	@RequestMapping("/getTotalCountByBar.action")
	public JSONObject getTotalCountByBar(String startDate,String endDate){
		try{
			logger.info("获取职位需求总量数据处理开始....");
			startDate = (null == startDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "0101" : formatDate(startDate);
			endDate = (null == endDate) ? DateUtils.getDateFormat(new Date(), "yyyy") + "1231" : formatDate(endDate);
			
			JSONObject object = mainService.getTotalCountByBar(startDate, endDate);
			logger.info("获取职位需求总量数据处理结束....");
			return object;
		}catch(Exception e){
			e.printStackTrace();
			logger.error("获取职位需求总量数据出错，出错原因："+e.getMessage());
		}
		return null;
	}
	
	public static void main(String[] args) {
		
//		Map<String,Integer> map = new HashMap<String, Integer>();
//		map.put("北京", 1);
//		map.put("上海", 1);
//		map.put("厦门", 1);
//		
//		List<JSONObject> objects = new ArrayList<JSONObject>();
//		JSONObject object = new JSONObject();
//		object.putAll(map);
//		objects.add(object);
//		System.out.println(objects.toString());
		
	}
}
