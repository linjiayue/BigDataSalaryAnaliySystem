package com.lin.service.impl;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lin.dao.MainDao;
import com.lin.service.AnalysisSalaryService;
import com.lin.utils.LoggerUtils;

@Service
public class AnalysisSalaryServiceImpl implements AnalysisSalaryService {
	@Autowired
	private MainDao mainDao;
	private Logger logger = LoggerUtils.getLogger("AnalysisSalaryServiceImpl");
	
	
}
