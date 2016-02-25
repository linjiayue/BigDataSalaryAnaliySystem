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
<style type="text/css">
	body{margin:0;padding:0;position:relative;}
	#box{width:1000px;position: absolute;margin-left: 200px;}
</style>
<script type="text/javascript">
	$(function(){
		
		
	});
</script>
</head>
<body>
	<div id="box">
		<div>
			<div style="font-size:24px; color:#2984AB; border-bottom:solid 1px #2984AB; margin:10px;">各省份职位薪资水平：</div>
		</div>
		<div id="totalMap" style="height:400px;width:800px;"></div>
	</div>
</body>
</html>