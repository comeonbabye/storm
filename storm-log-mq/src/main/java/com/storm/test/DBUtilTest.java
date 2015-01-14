package com.storm.test;

import java.sql.ResultSet;

import com.storm.util.DBUtil;

public class DBUtilTest {

	public static void main(String[] args) {
		
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

}
