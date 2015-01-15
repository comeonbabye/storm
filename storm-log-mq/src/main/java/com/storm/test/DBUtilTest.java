package com.storm.test;

import java.sql.ResultSet;

import backtype.storm.Config;

import com.storm.repository.DBUtil;
import com.storm.util.Constants;

public class DBUtilTest {

	
	public static void test() {
		DBUtil db = new DBUtil();

		ResultSet rs = db.executeQuery("select id, db_key, db_name, db_date, db_count from db_count where db_key=" + 123, 10);
		
		
		try {
			if(rs.next()) {
				System.out.println(rs.getLong("id"));
			}  
		} catch (Exception e) {
			e.printStackTrace();
			 
		}
	}
	
	public static void test1() {
		
		Config stormConf = new Config();
		stormConf.setDebug(true);
		stormConf.put(Constants.DB_HOST, "42.96.168.163");
		stormConf.put(Constants.DB_PORT, "3306");
		stormConf.put(Constants.DB_NAME, "illidan_one");
		stormConf.put(Constants.DB_USER_NAME, "root");
		stormConf.put(Constants.DB_PASSWORD, "123456");
		
		DBUtil db = new DBUtil((String)stormConf.get(Constants.DB_HOST), 
				(String)stormConf.get(Constants.DB_PORT), 
				(String)stormConf.get(Constants.DB_NAME), 
				(String)stormConf.get(Constants.DB_USER_NAME), 
				(String)stormConf.get(Constants.DB_PASSWORD));
		
		ResultSet rs = db.executeQuery("select id, db_key, db_name, db_date, db_count from db_count where db_key='db'", 10);
		
		try {
			if(rs.next()) {
				System.out.println(rs.getLong("id"));
			}  
		} catch (Exception e) {
			e.printStackTrace();
			 
		}
		
	}
	
	public static void main(String[] args) {
		
		
		test1();
	}

}
