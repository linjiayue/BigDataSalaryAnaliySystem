package com.lin.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.hbase.HBaseConfiguration;

public class DateUtils {
	
	 /** 数据库存储的时间格式串，如yyyymmdd 或yyyymmddHHMiSS */
    public static final int DB_STORE_DATE = 1;

    /** 用连字符-分隔的时间时间格式串，如yyyy-mm-dd 或yyyy-mm-dd HH:Mi:SS */
    public static final int HYPHEN_DISPLAY_DATE = 2;

    /** 用连字符.分隔的时间时间格式串，如yyyy.mm.dd 或yyyy.mm.dd HH:Mi:SS */
    public static final int DOT_DISPLAY_DATE = 3;

    /** 用中文字符分隔的时间格式串，如yyyy年mm月dd 或yyyy年mm月dd HH:Mi:SS */
    public static final int CN_DISPLAY_DATE = 4;
    /** 日期的开始时间戳*/
    public static final String DB_STORE_DATE_BEGIN = "000000";
    /** 日期的结束时间戳*/
    public static final String DB_STORE_DATE_END = "235959";
	
	/**
	 * 
	 * @param date
     *            Date 需要调整的日期时间对象
     * @param calendarField
     *            int 对日期时间对象以什么单位进行调整：
     * 
     * <pre>
     * &lt;blockquote&gt;
     * 年 Calendar.YEAR
     * 月 Calendar.MONTH
     * 日 Calendar.DATE
     * 时 Calendar.HOUR
     * 分 Calendar.MINUTE
     * 秒 Calendar.SECOND
     * &lt;/blockquote&gt;
     * </pre>
     * 
     * @param amount
     *            int 调整数量，>0表向后调整（明天，明年），<0表向前调整（昨天，去年）
     * @return Date 调整后的日期时间对象
	 * @return
	 */
	public static Date addDate(Date date, int calendarField, int amount) {
        if (null == date) {
            return date;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(calendarField, amount);
        return calendar.getTime();
    }
	
	/**
    * 获取昨天的日期
    * @param formatType
    * @return
    * @author:liwl
    */
    public static String getYestoday(int formatType){
        String yestoday="";
		Calendar todayDate = Calendar.getInstance();
		todayDate.add(Calendar.DAY_OF_MONTH, -1);
	    yestoday=DateUtils.getDateStr(todayDate.getTime(),formatType);//yyyyMMdd
    	return yestoday;
    }
    
    /**
     * 得到精确到天的指定时间格式化日期串
     * 
     * @param date
     *            指定时间
     * @param formatType
     *            时间格式的类型{@link #DB_STORE_DATE},{@link #EN_HTML_DISPLAY_DATE},{@link #CN_HTML_DISPLAY_DATE}
     * @return 指定时间格式化日期串
     */
    public static String getDateStr(Date date, int formatType) {
        if (formatType < DB_STORE_DATE || formatType > CN_DISPLAY_DATE) {
            throw new IllegalArgumentException("时间格式化类型不是合法的值。");
        } else {
            String formatStr = null;
            switch (formatType) {
            case DB_STORE_DATE:
                formatStr = "yyyyMMdd";
                break;
            case HYPHEN_DISPLAY_DATE:
                formatStr = "yyyy-MM-dd";
                break;
            case DOT_DISPLAY_DATE:
                formatStr = "yyyy.MM.dd";
                break;
            case CN_DISPLAY_DATE:
                formatStr = "yyyy'年'MM'月'dd";
                break;
            default:
                formatStr = "yyyyMMdd";
                break;
            }
            SimpleDateFormat sdf = new SimpleDateFormat(formatStr);
            return sdf.format(date);
        }
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
    
	public static void main(String[] args) {
		System.out.println(DateUtils.getYestoday(2));
	}
}
