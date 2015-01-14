package com.repository;

import java.sql.ResultSet;
import java.util.Date;

import com.entitiy.Item;
import com.util.DBUtil;

public class ItemRepository {

	private DBUtil db;
	
	public ItemRepository() {
		
	}
	
	public ItemRepository(DBUtil db) {
		
		this.db = db;
	}
	
	public void update(String sql) {

		db.executeUpdate(sql);
	}
	
	public void saveOrUpdate(String sql, Object ... objs ) {
		db.executeUpdate2(sql, objs);
	}
 
	
	public Long getItemId(String key) {
		
		Long id = -1L;
		
		ResultSet rs = null;
		try {
			rs = db.executeQuery("select id from db_count where db_key='" + key + "'", 10);
			
			if(rs != null && rs.next()) {
				id = rs.getLong(1);
			}
		} catch (Exception e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
		return id;
	}
	
	public boolean isExsitsItem(String key) {
		
		ResultSet rs = null;
		try {
			rs = db.executeQuery("select count(*) from db_count where db_key='" + key + "'", 10);
			if(rs != null && rs.next()) {
				int count = rs.getInt(1);
				return count > 0 ? true : false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			
		} finally {
			try {
				if(rs != null) {
					rs.close();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		
		return false;
	}
	
	public Item getItem(String dbName, String key, Date today) {
		
		ResultSet rs = db.executeQuery("select id, db_key, db_name, db_date, db_count from db_count where db_key='" + key + "'", 10);
		
		Item item = new Item();
		
		try {
			if(rs.next()) {
				item.setId(rs.getLong("id"));
				item.setDbKey(rs.getString("db_key"));
				item.setDbDate(rs.getDate("db_date"));
				item.setDbName(rs.getString("db_name"));
				item.setDbCount(rs.getInt("db_count"));
			} else {
				item.setId(-1L);
				item.setDbCount(0);
				item.setDbKey(key);
				item.setDbDate(today);
				item.setDbName(dbName);
			}
		} catch (Exception e) {
			e.printStackTrace();
			item.setId(-1L);
			item.setDbCount(0);
			item.setDbKey(key);
			item.setDbDate(today);
			item.setDbName(dbName);
		}
		
		return item;
	}
}
