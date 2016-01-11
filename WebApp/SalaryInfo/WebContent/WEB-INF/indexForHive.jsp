<%@ page language="java" contentType="text/html; charset=utf-8" pageEncoding="utf-8"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<title>大数据薪资分析系统</title>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/jquery.min.js"></script>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/common.js"></script>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/echarts/echarts-all.js"></script>
<script type="text/javascript" src="<%=request.getContextPath()%>/js/My97DatePicker/WdatePicker.js"></script>
</head>
<style type="text/css">
	body{margin:0;padding:0;position:relative;}
	#box{width:1000px;position: absolute;margin-left: 200px;}
</style>
<script type="text/javascript">
$(function(){
	var mapURL = "${pageContext.request.contextPath}/mainController/getTotalCountByMapUHive.action";
	var barURL = "${pageContext.request.contextPath}/mainController/getTotalCountByBarUHive.action";
	var lineURL = "${pageContext.request.contextPath}/mainController/getBSGSInfoUHive.action";
	//总需求量地图表示
   function totalMap(myChart,dt){
	   option = {
			    title : {
			        text: '大数据相关职位需求量',
			        subtext: '数据来自拉钩网',
			        x:'center'
			    },
			    tooltip : {
			        trigger: 'item'
			    },
			    legend: {
			        orient: 'vertical',
			        x:'left',
			        data:['Hadoop','Spark','数据挖掘','数据分析']
			    },
			    dataRange: {
			        x: 'left',
			        y: 'bottom',
			        splitList: [
			            {start: 500},
			            {start: 300, end: 500},
			            {start: 200, end: 300},
			            {start: 100, end: 200},
			            {start: 50, end: 100},
			            {start: 1, end: 50},
			            {end: 1}
			            
			        ],
			        color: ['#E0022B', '#E09107', '#A3E00B']
			    },
			    roamController: {
			        show: true,
			        x: 'right',
			        mapTypeControl: {
			            'china': true
			        }
			    },
			    series : [
			        {
			            name: 'Hadoop',
			            type: 'map',
			            mapType: 'china',
			            roam: false,
			            itemStyle:{
			                normal:{
			                    label:{
			                        show:true,
			                        textStyle: {
			                           color: "rgb(249, 249, 249)"
			                        }
			                    }
			                },
			                emphasis:{label:{show:true}}
			            },
			            data:dt.Hadoop
			        },
			        {
			            name: 'Spark',
			            type: 'map',
			            mapType: 'china',
			            roam: false,
			            itemStyle:{
			                normal:{
			                    label:{
			                        show:true,
			                        textStyle: {
			                           color: "rgb(249, 249, 249)"
			                        }
			                    }
			                },
			                emphasis:{label:{show:true}}
			            },
			            data:dt.Spark
			        },
			        {
			            name: '数据挖掘',
			            type: 'map',
			            mapType: 'china',
			            roam: false,
			            itemStyle:{
			                normal:{
			                    label:{
			                        show:true,
			                        textStyle: {
			                           color: "rgb(249, 249, 249)"
			                        }
			                    }
			                },
			                emphasis:{label:{show:true}}
			            },
			            data:dt.DM
			        },
			        {
			            name: '数据分析',
			            type: 'map',
			            mapType: 'china',
			            roam: false,
			            itemStyle:{
			                normal:{
			                    label:{
			                        show:true,
			                        textStyle: {
			                           color: "rgb(249, 249, 249)"
			                        }
			                    }
			                },
			                emphasis:{label:{show:true}}
			            },
			            data:dt.DA
			        }
			    ]
			};
	       // 为echarts对象加载数据 
	       myChart.setOption(option); 
	   
   }
   
   function totalMapAjax(){
	   var myChart = echarts.init(document.getElementById("totalMap"));
	   ajaxUtilShowLoading(mapURL,myChart,totalMap);
   }
   totalMapAjax();
   //职位需求量柱状图：
   function totalBar(myChart,dt){
	   var option = {
	            tooltip: {
	                show: true
	            },
	            legend: {
	                data:['职位需求量']
	            },
	            xAxis : [
	                {
	                    type : 'category',
	                    data : ["Hadoop","Spark","数据挖掘","数据分析"]
	                }
	            ],
	            yAxis : [
	                {
	                    type : 'value'
	                }
	            ],
	            series : [
	                {
	                    "name":"职位需求量",
	                    "type":"bar",
	                    "data":[(dt.Hadoop != undefined) ? dt.Hadoop : 0, (dt.Spark != undefined) ? dt.Spark : 0, (dt.DM != undefined) ? dt.DM : 0, (dt.DA != undefined) ? dt.DA : 0]
	                },
	            ]
	        };
			                    
		// 为echarts对象加载数据 
      myChart.setOption(option); 
   }
   
   function totalBarAjax(){
	   var myChart = echarts.init(document.getElementById("totalBar"));
	 //职位需求量柱状图：
     ajaxUtilShowLoading(barURL,myChart,totalBar);
	 
   }
   totalBarAjax();
   
   //职位需求量线状图：
   function totalLine(myChart,dt){
	   option = {
			    tooltip : {
			        trigger: 'axis'
			    },
			    legend: {
			        data:['Hadoop','Spark',"数据挖掘","数据分析"]
			    },
			    calculable : true,
			    xAxis : [
			        {
			            type : 'category',
			            boundaryGap : false,
			            data : ['广州','深圳','上海','北京']
			        }
			    ],
			    yAxis : [
			        {
			            type : 'value'
			        }
			    ],
			    series : [
			        {
			            name:'Hadoop',
			            type:'line',
			            stack: '总量',
			            data:[(dt.Hadoop.GZ != undefined) ? dt.Hadoop.GZ : 0, (dt.Hadoop.SZ != undefined) ? dt.Hadoop.SZ : 0,(dt.Hadoop.SH != undefined) ? dt.Hadoop.SH : 0,(dt.Hadoop.BJ != undefined) ? dt.Hadoop.BJ : 0]
			        },
			        {
			            name:'Spark',
			            type:'line',
			            stack: '总量',
			            data:[(dt.Spark.GZ != undefined) ? dt.Spark.GZ : 0, (dt.Spark.SZ != undefined) ? dt.Spark.SZ : 0,(dt.Spark.SH != undefined) ? dt.Spark.SH : 0,(dt.Spark.BJ != undefined) ? dt.Spark.BJ : 0]
			        },
			        {
			            name:'数据挖掘',
			            type:'line',
			            stack: '总量',
			            data:[(dt.DM.GZ != undefined) ? dt.DM.GZ : 0, (dt.DM.SZ != undefined) ? dt.DM.SZ : 0,(dt.DM.SH != undefined) ? dt.DM.SH : 0,(dt.DM.BJ != undefined) ? dt.DM.BJ : 0]
			        },
			        {
			            name:'数据分析',
			            type:'line',
			            stack: '总量',
			            data:[(dt.DA.GZ != undefined) ? dt.DA.GZ : 0, (dt.DA.SZ != undefined) ? dt.DA.SZ : 0,(dt.DA.SH != undefined) ? dt.DA.SH : 0,(dt.DA.BJ != undefined) ? dt.DA.BJ : 0]
			        }
			    ]
			};
			                    
	// 为echarts对象加载数据 
	myChart.setOption(option); 
   }
  function totalLineAjax(){
	  var myChart = echarts.init(document.getElementById("totalLine"));
	
    ajaxUtilShowLoading(lineURL,myChart,totalLine);
  }
  totalLineAjax();
   
  
  $("#mapSearch").click(function(){
	  var startDate = $("#startDateMap").val();
	  if(undefined != startDate && "" != startDate){
	  	var myChart = echarts.init(document.getElementById("totalMap"));
	  	myChart.showLoading({text : "别捉急,正在跑呢........"});
		//总需求量地图表示
			$.ajax({
				url : mapURL,
				data : {
					startDate : startDate,
				},
				dataType : 'json',
				success : function(data) {
					myChart.hideLoading();
					totalMap(myChart,data);
				}
			});	
	  }
	  
  });
  
   	$("#barSearch").click(function(){
   		var startDate = $("#startBar").val();
   		var endDate = $("#endDateBar").val();
   		if(undefined != startDate && "" != startDate && undefined != endDate && "" != endDate){
   			var myChart = echarts.init(document.getElementById("totalBar"));
   			myChart.showLoading({text : "别捉急,正在跑呢........"});
   			$.ajax({
   				url : barURL,
   				data : {
   					startDate : startDate,
   					endDate : endDate
   				},
   				dataType : 'json',
   				success : function(data) {
   					myChart.hideLoading();
   					totalBar(myChart,data);
   				}
   			});	
   		}
   	});
   	
   	$("#lineSearch").click(function(){
   		var startDate = $("#startDateLine").val();
   		var endDate = $("#endDateLine").val();
   		if(undefined != startDate && "" != startDate && undefined != endDate && "" != endDate){
   			var myChart = echarts.init(document.getElementById("totalLine"));
   			myChart.showLoading({text : "别捉急,正在跑呢........"});
   			$.ajax({
   				url : lineURL,
   				data : {
   					startDate : startDate,
   					endDate : endDate
   				},
   				dataType : 'json',
   				success : function(data) {
   					myChart.hideLoading();
   					totalLine(myChart,data);
   				}
   			});	
   		}
   	});
})
	
</script>
<body>
<div id="box">
	<div>
		<div style="font-size:24px; color:#2984AB; border-bottom:solid 1px #2984AB; margin:10px;">各省份职位需求量：</div>
			<div style="font-size:18px;margin-left:10px;font-weight: bold;margin-bottom:10px;">
				年份：<input name="startDate" id="startDateMap" type="text" style="width:200px;height:18px;" onclick="WdatePicker({dateFmt: 'yyyy'})"/>
				<input type="button" value="查询" id="mapSearch" >
			</div>
	</div>
	<div id="totalMap" style="height:400px;width:800px;"></div>
	<div>
		<div style="font-size:24px; color:#2984AB; border-bottom:solid 1px #2984AB; margin:10px;">各职位需求总量对比：</div>
			<div style="font-size:18px;margin-left:10px;font-weight: bold;margin-bottom:10px;">
				时间：<input name="startDate" id="startBar" type="text" style="width:200px;height:18px;" onclick="WdatePicker({dateFmt: 'yyyy-MM-dd',maxDate:'#F{$dp.$D(\'endDateBar\')}'})"/> -
				<input name="endDate" type="text" id="endDateBar" class="input-line" style="width:200px;height:18px;" onclick="WdatePicker({dateFmt: 'yyyy-MM-dd',minDate:'#F{$dp.$D(\'startBar\')}'})"/>
				<input type="button" value="查询" id="barSearch" >
			</div>
	</div>
	<div id="totalBar" style="height:400px;width:800px;"></div>
	<div>
		<div style="font-size:24px; color:#2984AB; border-bottom:solid 1px #2984AB; margin:10px;">北上广深职位需求量对比堆积折线图：</div>
			<div style="font-size:18px;margin-left:10px;font-weight: bold;margin-bottom:10px;">
				时间：<input name="startDate" id="startDateLine" type="text" style="width:200px;height:18px;" onclick="WdatePicker({dateFmt: 'yyyy-MM-dd',maxDate:'#F{$dp.$D(\'endDateLine\')}'})"/> -
				<input name="endDate" type="text" id="endDateLine" class="input-line" style="width:200px;height:18px;" onclick="WdatePicker({dateFmt: 'yyyy-MM-dd',minDate:'#F{$dp.$D(\'startDateLine\')}'})"/>
				<input type="button" value="查询" id="lineSearch" >
			</div>
	</div>
	<div id="totalLine" style="height:400px;width:800px;"></div>
</div>
</body>
</html>