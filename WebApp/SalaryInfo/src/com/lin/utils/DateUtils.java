package com.lin.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {
	
	/**
	 * Add by linjy on 2016-01-06
	 * @param formatStr	需要获得的字符串格式
	 * @return	返回指定格式的昨天日期
	 */
	public static String getYest0day(String formatStr){
		Calendar todayDate = Calendar.getInstance();
		todayDate.add(Calendar.DAY_OF_MONTH, -1);
		SimpleDateFormat format = new SimpleDateFormat(formatStr);
		String yesToday = format.format(todayDate.getTime());
		return yesToday;
	}
	/**
	 * Add by linjy on 2016-01-06
	 * @param date	指定日期
	 * @param formatStr	需要获得的字符串格式
	 * @return	返回指定格式的日期
	 */
	public static String getDateFormat(Date date,String formatStr){
		SimpleDateFormat format = new SimpleDateFormat(formatStr);
		return format.format(date);
	}
}
