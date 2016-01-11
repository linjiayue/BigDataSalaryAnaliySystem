##大数据招聘信息分析平台##
这是依据爬取程序,爬取到的招聘信息进行各维度的分析及展现最终结果的平台

###平台环境###
- Centos 7
- Hadoop-2.5.1
- Zookeeper-3.4.6
- HBase-1.1.2
- Hive-1.2.1
- MySql-5.6
- Python 3.5
- JDK 1.8

搭建过程可参考:[我的博客](http://blog.csdn.net/u011523533/article/category/5986195)

###平台项目结构###

- 爬取项目(爬虫),使用Python编写
- 加载数据到HBase数据库项目,使用Java编写
- Web端展现项目,使用Java编写

###各项目功能###

1.爬取项目

- 定时爬取指定连接的内容,并将所爬取到的内容保存到指定文件夹中
- 调用加载到HBase数据库的项目(Jar),使数据加载到HBase中

2.加载数据到HBase数据库项目

- 读取指定目录下的文件,分析其中的内容并且保存到HBase中
- 多线程加载

3.Web端展现项目

- 图形化展现分析数据后的结果
- 定时执行统计程序,并将结果保存到结果表中

###平台现有功能###

- 爬取脚本爬取拉钩网指定关键词(Hadoop,Spark,数据分析,数据挖掘)的内容,并将爬取到的内容已指定的格式存储在文件中,目前存储的格式为JSON字符串格式
- 爬取脚本调用加载数据到HBase中的Jar
- 分析指定文件夹中的文件内容,将其转化为JSON后,映射HBase中的表字段存储内容
- Web应用中,定时执行统计程序,并将统计结果保存到HBase的结果表中,目前已有的统计内容为统计每天,每个城市,每个职位(Hadoop,Spark,数据分析,数据挖掘)的职位数
- 目前已有的展示内容为,以中国地图的形式展现各省份各职位数数量,以柱状图的形式展示各职位(Hadoop,Spark,数据分析,数据挖掘)的职位数,以堆积折线图的形式展示北上广深四个城市各职位(Hadoop,Spark,数据分析,数据挖掘)数量

###建表脚本###

####HBase####
```create 'SalaryInfoResult','ResultInfoFamily'```

```create 'SalaryInfo','PositionInfoFamily','CompanyInfoFamily','OtherInfoFamily'```

####Hive####
```create external table SalaryInfo(key string,positionName string,createTime string,createDate string,insertDate string,salary string,workYear string,city string,createTimeSort string,companySize string,financeStage string,education string,positionAdvantage string,positionType string,industryField string,companyLabelList string,companyName string,companyShortName string,jobNature string,positionFirstType string,leaderName string,flowScore string,searchScore string,countAdjusted string,pvScore string,companyLogo string,positonTypesMap string,orderBy string,formatCreateTime string,haveDeliver string,adWord string,score string,positionId string,deliverCount string,relScore string,totalCount string,showOrder string,showCount string,calcScore string,companyId string,randomScore string,hrScore string,adjustScore string,imstate string,plus string)stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' with serdeproperties("hbase.columns.mapping"=":key,PositionInfoFamily:positionName,PositionInfoFamily:createTime,PositionInfoFamily:createDate,PositionInfoFamily:insertDate,PositionInfoFamily:salary,PositionInfoFamily:workYear,PositionInfoFamily:city,PositionInfoFamily:createTimeSort,CompanyInfoFamily:companySize,CompanyInfoFamily:financeStage,CompanyInfoFamily:education,CompanyInfoFamily:positionAdvantage,CompanyInfoFamily:positionType,CompanyInfoFamily:industryField,CompanyInfoFamily:companyLabelList,CompanyInfoFamily:companyName,CompanyInfoFamily:companyShortName,CompanyInfoFamily:jobNature,CompanyInfoFamily:positionFirstType,CompanyInfoFamily:leaderName,OtherInfoFamily:flowScore,OtherInfoFamily:searchScore,OtherInfoFamily:countAdjusted,OtherInfoFamily:pvScore,OtherInfoFamily:companyLogo,OtherInfoFamily:positonTypesMap,OtherInfoFamily:orderBy,OtherInfoFamily:formatCreateTime,OtherInfoFamily:haveDeliver,OtherInfoFamily:adWord,OtherInfoFamily:score,OtherInfoFamily:positionId,OtherInfoFamily:deliverCount,OtherInfoFamily:relScore,OtherInfoFamily:totalCount,OtherInfoFamily:showOrder,OtherInfoFamily:showCount,OtherInfoFamily:calcScore,OtherInfoFamily:companyId,OtherInfoFamily:randomScore,OtherInfoFamily:hrScore,OtherInfoFamily:adjustScore,OtherInfoFamily:imstate,OtherInfoFamily:plus")tblproperties("hbase.table.name"="SalaryInfo");```

##更新历史##
###版本:1.0 更新时间 2015-12-26###


