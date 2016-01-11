function ajaxUtil(uri,doFunction){
 	$.ajax({
		url : uri,
		dataType : 'json',
		success : function(data) {
//			console.info(data);
			//doFunction(eval(data));
			doFunction(data);
		}
	});	
	
}

function ajaxUtilShowLoading(uri,myChart,doFunction){
	myChart.showLoading({text : "别捉急,正在跑呢........"});
 	$.ajax({
		url : uri,
		dataType : 'json',
		success : function(data) {
			myChart.hideLoading();
			doFunction(myChart,data);
		}
	});	
	
}