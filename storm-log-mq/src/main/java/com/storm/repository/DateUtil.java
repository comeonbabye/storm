
package com.storm.repository;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;


/**
 * 
 * @author tony.he
 */
public class DateUtil {

    public static Date startOfDay(Date date) {
        return new DateTime(date).withMillisOfDay(0).toDate();
    }

    public static Date endOfDay(Date date) {
        return new DateTime(date).withMillisOfDay(DateTimeConstants.MILLIS_PER_DAY - 1).toDate();
    }
    public static Date startOfWeek(Date date) {
    	return new DateTime(date).withDayOfWeek(1).withMillisOfDay(0).toDate();
    }
    public static Date endOfWeek(Date date) {
    	return endOfDay(DateUtil.beforeDays(DateUtil.afterWeeks(DateUtil.startOfWeek(date), 1), 1));
    }
    public static Date startOfMonth(Date date) {
        return new DateTime(date).withDayOfMonth(1).withMillisOfDay(0).toDate();
    }

    public static Date endOfMonth(Date date) {
    	return endOfDay(DateUtil.beforeDays(DateUtil.afterMonths(DateUtil.startOfMonth(date), 1), 1));
    }
    public static Date startOfYear(Date date) {
        return new DateTime(date).withDayOfYear(1).withMillisOfDay(0).toDate();
    }

    public static Date endOfYear(Date date) {
    	return endOfDay(DateUtil.beforeDays(DateUtil.afterYears(DateUtil.startOfYear(date), 1), 1));
    }
    
    public static Date beforeDays(Date date, int day) {
        return new DateTime(date).minusDays(day).toDate();
    }

    public static Date beforeMonths(Date date, int months) {
        return new DateTime(date).minusMonths(months).toDate();
    }

    public static Date beforeYears(Date date, int years) {
        return new DateTime(date).minusYears(years).toDate();
    }

    public static Date afterDays(Date date, int day) {
        return new DateTime(date).plusDays(day).toDate();
    }
    public static Date afterWeeks(Date date, int weeks) {
        return new DateTime(date).plusWeeks(weeks).toDate();
    }
    public static Date afterMonths(Date date, int months) {
        return new DateTime(date).plusMonths(months).toDate();
    }

    public static Date afterYears(Date date, int year) {
        return new DateTime(date).plusYears(year).toDate();
    }

    
    public static Date generateStart(Date end, String cycle) {

        Date start = null;

        if("month".equalsIgnoreCase(cycle)) {
        	start = DateUtil.beforeMonths(end, 1);
        } else if("week".equalsIgnoreCase(cycle)) {
        	start = DateUtil.beforeDays(end, 7);
        } else if("year".equalsIgnoreCase(cycle)) {
        	start = DateUtil.beforeYears(end, 1);
        }  else if("day".equalsIgnoreCase(cycle)) {
        	start = DateUtil.beforeDays(end, 1);
        }

        return start;
    }
    
    /**
     * format date to string as "yyyy-MM-dd"
     * @param date
     * @return "yyyy-MM-dd"
     */
    public static String dateToString(Date date){
		return dateToString(date,"yyyy-MM-dd");
    }
    public static String dateToString(Date date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
    }
    public static Date stringToDate(String dateStr,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.parse(dateStr);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
    }   
    
    /**
     * format string as "yyyy-MM-dd" to Date
     * @param date
     * @return "yyyy-MM-dd"
     */   
    public static Date stringToDate(String dateStr){
    	
    	return stringToDate(dateStr,"yyyy-MM-dd");
    }     
    
    public static Date getFirstDayOfMonth(int year, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month - 1);
		calendar.set(Calendar.DAY_OF_MONTH, calendar.getMinimum(Calendar.DATE));
		return calendar.getTime();
	}

	public static Date getLastDayOfMonth(int year, int month) {
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.YEAR, year);
		calendar.set(Calendar.MONTH, month - 1);
		calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DATE));
		return calendar.getTime();
	}
    
    public static final String DATE_FORMAT_1 = "yyyy-MM-dd HH:mm:ss";
    public static final String DATE_FORMAT_2 = "yyyy-MM-dd";
    public static final String DATE_FORMAT_3 = "yyyyMMddhhmmss";
    public static final String DATE_FORMAT_4 = "yyyy年MM月dd�?";
    public static final String DATE_FORMAT_5 = "yyyyMMdd";
}
