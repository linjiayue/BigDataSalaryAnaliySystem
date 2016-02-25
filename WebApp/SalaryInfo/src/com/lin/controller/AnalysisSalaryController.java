package com.lin.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

import com.lin.service.AnalysisSalaryService;
import com.lin.utils.LoggerUtils;
/**
 * Add by linjy on 2016-2-22
 * 薪资分析控制类
 * @author linjy
 *
 */
@Controller
@RequestMapping("/analysisSalaryController")
public class AnalysisSalaryController {
	@Autowired
	private AnalysisSalaryService analysisSalaryService;
	
	private Logger logger = LoggerUtils.getLogger("AnalysisSalaryController");
	
	
}
