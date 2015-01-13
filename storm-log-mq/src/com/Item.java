package com;

import java.util.Date;

@SuppressWarnings("serial")
public class Item implements java.io.Serializable {

	private Long id;
	private String dbKey;
	private String dbName;
	private Date dbDate;
	private Integer count;
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getDbKey() {
		return dbKey;
	}
	public void setDbKey(String dbKey) {
		this.dbKey = dbKey;
	}
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public Date getDbDate() {
		return dbDate;
	}
	public void setDbDate(Date dbDate) {
		this.dbDate = dbDate;
	}
	public Integer getCount() {
		return count;
	}
	public void setCount(Integer count) {
		this.count = count;
	}
}
