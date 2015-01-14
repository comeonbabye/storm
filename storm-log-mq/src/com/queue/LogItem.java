package com.queue;

import java.io.Serializable;
import org.apache.commons.lang.builder.ToStringBuilder;

public class LogItem implements Serializable {

    private static final long serialVersionUID = 225642231949165835L;

    private String dbName;
    private String url;
	public String getDbName() {
		return dbName;
	}
	public void setDbName(String dbName) {
		this.dbName = dbName;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	
	/**
	 * @see java.lang.Object#toString()
	 */
	public String toString() {
		return new ToStringBuilder(this).append("url", this.url).append("dbName", this.dbName).toString();
	}
}
