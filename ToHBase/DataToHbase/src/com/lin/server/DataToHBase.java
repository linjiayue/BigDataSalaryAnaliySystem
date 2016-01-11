package com.lin.server;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

import com.lin.utils.DateUtils;
import com.lin.utils.HBaseUtils;
import com.lin.utils.LoggerUtils;

/**
 * Add by linjy on 2015-12-15
 * @author linjy
 *	加载数据到HBase实现类
 */
public class DataToHBase {
	
	private Logger logger = null;
	//保存文件名对应的缩写
	private Map<String,String> dirFileName = new HashMap<String, String>();
	
	public DataToHBase(){
		logger = LoggerUtils.getLogger("DataToHBase");
		dirFileName.put("Hadoop", "hp");
		dirFileName.put("Spark", "sp");
//		//数据分析
//		dirFileName.put("DataAnalySis", "da");
//		//数据挖掘
//		dirFileName.put("DataMining", "dm");
		dirFileName.put("数据挖掘", "da");
		dirFileName.put("数据分析", "dm");
	}
	/**
	 * Add by linjy on 2015-12-15
	 * @param filePath	需要加载的文件地址
	 * @param isFist	是否是第一次加载数据
	 * 加载文件数据到HBase中
	 */
	public void loadData(String filePath){
		logger.info("开始加载"+filePath+"数据.....");
		long startTime = System.currentTimeMillis();
		Connection conn = null;
		try{
			conn = HBaseUtils.getConnection();
				
			logger.info("加载数据开始.....");
			loadDataFist(filePath,conn);
			logger.info("加载数据结束.....");
		}catch(Exception e){
			e.printStackTrace();
			logger.error("加载数据出错");
			logger.error(e.getMessage());
		}finally{
			try {
				HBaseUtils.closeConn(conn);
			} catch (IOException e) {
				e.printStackTrace();
				logger.error("关闭连接出错");
				logger.error(e.getMessage());
			}
		}
		long endTime = System.currentTimeMillis();
		logger.info("加载"+filePath+"数据结束.....,共花费时间：" + (endTime - startTime) + "毫秒");
	}
	/**
	 * Add by linjy on 2015-12-15
	 * @param filePath	文件名
	 * @param conn	Hbase连接
	 * 将所有数据加载到HBase中
	 * @throws FileNotFoundException 
	 */
	private void loadDataFist(String filePath,Connection conn) throws Exception {
		int count = 0;
		BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)));
		String data = reader.readLine();
		while(null != data){
			//转化数据
			Map<String,Object> dataMap = formatStr(data);
			//数据清洗，按一定规则筛选数据
			if(cleanData(dataMap)){
				//保存数据
				saveDataToHBase(dataMap,conn);
				count ++;
			}
			data = reader.readLine();
		}
		logger.info("读取数据加载到HBase中结果，读取路劲为：" + filePath + ",数量为：" + count);
	}
	
	/**
	 * Add by linjy on 2015-12-15
	 * @param dataMap	需要筛选的数据
	 * @return
	 * True 可以进行保存操作		False 筛除
	 */
	private boolean cleanData(Map<String, Object> dataMap) {
		//目前规则为，PositionInfoFamily 下不能有空值
		if(StringUtils.isBlank(dataMap.get("positionName").toString())){
			return false;
		}
		if(StringUtils.isBlank(dataMap.get("createTime").toString())){
			return false;
		}
		if(StringUtils.isBlank(dataMap.get("salary").toString())){
			return false;
		}
		if(StringUtils.isBlank(dataMap.get("workYear").toString())){
			return false;
		}
		if(StringUtils.isBlank(dataMap.get("city").toString())){
			return false;
		}
		if(StringUtils.isBlank(dataMap.get("createTimeSort").toString())){
			return false;
		}
		
		return true;
	}
	/**
	 * Add by linjy on 2015-12-15
	 * @param data	需要保存的数据
	 * 将读取的数据保存到HBase中
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	private void saveDataToHBase(Map<String,Object> dataMap,Connection conn) throws Exception {
		//获得  该记录创建时间  公司ID，组建rowKey
		String createDateTime =  dataMap.get("createTime").toString();
		String companyId =  dataMap.get("companyId").toString();
		//获得创建日期
		String[] strTmp = (createDateTime.split(" ")[0]).split("-");
		String createDate = strTmp[0] + strTmp[1] + strTmp[2];
		
		//根据职位名称中包含的几个职位关键词来保存职位信息，如果一个职位名称中包含多个职位关键词，则按关键词保存信息
		Table table = conn.getTable(TableName.valueOf("SalaryInfo".getBytes()));
		List<String> pList = getPositionNames(dataMap.get("positionName").toString());
		List<Put> puts = new ArrayList<Put>();
		for(String positionName : pList){
			//组建rowKey
			String rowKey = createDate + dirFileName.get(positionName) + getRowKeySuffix(companyId);
			
			Put put = new Put(rowKey.getBytes());
			
			put.addColumn("PositionInfoFamily".getBytes(), "positionName".getBytes(), positionName.getBytes());
			put.addColumn("PositionInfoFamily".getBytes(), "createTime".getBytes(), (dataMap.get("createTime").toString()).getBytes());
			//保存创建日期
			put.addColumn("PositionInfoFamily".getBytes(), "createDate".getBytes(), createDate.getBytes());
			//保存数据插入时间
			put.addColumn("PositionInfoFamily".getBytes(), "insertDate".getBytes(), (DateUtils.getDateFormat(new Date(), "yyyyMMdd")).getBytes());
			put.addColumn("PositionInfoFamily".getBytes(), "salary".getBytes(), (dataMap.get("salary").toString()).getBytes());
			put.addColumn("PositionInfoFamily".getBytes(), "workYear".getBytes(), (dataMap.get("workYear").toString()).getBytes());
			put.addColumn("PositionInfoFamily".getBytes(), "city".getBytes(), (dataMap.get("city").toString()).getBytes());
			put.addColumn("PositionInfoFamily".getBytes(), "createTimeSort".getBytes(), (dataMap.get("createTimeSort").toString()).getBytes());
			
			put.addColumn("CompanyInfoFamily".getBytes(), "companySize".getBytes(), (dataMap.get("companySize").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "financeStage".getBytes(), (dataMap.get("financeStage").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "education".getBytes(), (dataMap.get("education").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "positionAdvantage".getBytes(), (dataMap.get("positionAdvantage").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "positionType".getBytes(), (dataMap.get("positionType").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "industryField".getBytes(), (dataMap.get("industryField").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "companyLabelList".getBytes(), (dataMap.get("companyLabelList").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "companyName".getBytes(), (dataMap.get("companyName").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "companyShortName".getBytes(), (dataMap.get("companyShortName").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "jobNature".getBytes(), (dataMap.get("jobNature").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "positionFirstType".getBytes(), (dataMap.get("positionFirstType").toString()).getBytes());
			put.addColumn("CompanyInfoFamily".getBytes(), "leaderName".getBytes(), (dataMap.get("leaderName").toString()).getBytes());
			
			put.addColumn("OtherInfoFamily".getBytes(), "flowScore".getBytes(), (dataMap.get("flowScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "searchScore".getBytes(), (dataMap.get("searchScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "countAdjusted".getBytes(), (dataMap.get("countAdjusted").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "pvScore".getBytes(), (dataMap.get("pvScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "companyLogo".getBytes(), (dataMap.get("companyLogo").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "positonTypesMap".getBytes(), (dataMap.get("positonTypesMap").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "orderBy".getBytes(), (dataMap.get("orderBy").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "formatCreateTime".getBytes(), (dataMap.get("formatCreateTime").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "haveDeliver".getBytes(), (dataMap.get("haveDeliver").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "adWord".getBytes(), (dataMap.get("adWord").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "score".getBytes(), (dataMap.get("score").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "positionId".getBytes(), (dataMap.get("positionId").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "deliverCount".getBytes(), (dataMap.get("deliverCount").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "relScore".getBytes(), (dataMap.get("relScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "totalCount".getBytes(), (dataMap.get("totalCount").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "showOrder".getBytes(), (dataMap.get("showOrder").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "showCount".getBytes(), (dataMap.get("showCount").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "calcScore".getBytes(), (dataMap.get("calcScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "companyId".getBytes(), (dataMap.get("companyId").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "randomScore".getBytes(), (dataMap.get("randomScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "hrScore".getBytes(), (dataMap.get("hrScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "adjustScore".getBytes(), (dataMap.get("adjustScore").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "imstate".getBytes(), (dataMap.get("imstate").toString()).getBytes());
			put.addColumn("OtherInfoFamily".getBytes(), "plus".getBytes(), (dataMap.get("plus").toString()).getBytes());
			
			puts.add(put);
		}
		//插入数据
		table.put(puts);
		//刷新缓冲区
		table.close();
	}
	/**
	 * Add by linjy on 2016-01-06
	 * @param positionName	职位名称
	 * @return	返回职位名称中包含的几个关键词
	 */
	private List<String> getPositionNames(String positionName){
		List<String> pList = new ArrayList<String>();
		if(positionName.toUpperCase().contains("Hadoop".toUpperCase())){
			pList.add("Hadoop");
		}
		if(positionName.toUpperCase().contains("Spark".toUpperCase())){
			pList.add("Spark");
		}
		if(positionName.contains("数据挖掘")){
			pList.add("数据挖掘");
		}
		if(positionName.contains("数据分析")){
			pList.add("数据分析");
		}
		return pList;
	}
	
	/**
	 * Add by linjy on 2015-12-15
	 * @param str	需要转换的JSON串
	 * @return
	 * 将JSON串转化为Map对象
	 */
	private Map<String,Object> formatStr(String str){
		//对JSON串进行数据处理，将None转化为''
		str = str.replaceAll("None", "''");
		JSONObject jObject = JSONObject.fromObject(str);
		Map<String, Object> map = (Map<String, Object>) JSONObject.toBean(jObject, Map.class);
		return map;
	}
	/**
	 * Add by linjy on 2015-12-15
	 * @param companyId	公司ID
	 * @return
	 * 生成固定位数的rowKey后缀
	 */
	private String getRowKeySuffix(String companyId){
		
		StringBuffer suffixRandDom = new StringBuffer();
		//在关键词后的，后缀为8位数，自动补全公司ID的位数
		int n = 8 - companyId.length();
		Random random = new Random();
		for(int i =0 ; i < n ; i++){
			suffixRandDom.append(random.nextInt(10)+"");
		}
		return companyId + suffixRandDom.toString();
		
	}
	
}
