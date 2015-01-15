package com.storm.repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/*数据库连接类*/
public class DBUtil {
	public Connection conn = null;
	public Statement stmt = null;
	private PreparedStatement  ps = null;
	public ResultSet rs = null;
	private String dbClassName = "com.mysql.jdbc.Driver";
	private String dbUrl = "jdbc:mysql://42.96.168.163:3306/illidan_one?user=root&password=123456&characterEncoding=utf-8&useUnicode=true";

	
	public DBUtil() {
		
	}
	
	public DBUtil(String host, String port, String dbName, String userName, String password) {
		
		StringBuffer sb = new StringBuffer();
		sb.append("jdbc:mysql://")
		.append(host).append(":")
		.append(port).append("/")
		.append(dbName).append("?")
		.append("user=").append(userName)
		.append("&password=").append(password)
		.append("&characterEncoding=utf-8&useUnicode=true");
		
		this.dbUrl = sb.toString();
	}
	
	public Connection getConnection() {

		if (conn != null) {
			return conn;
		}
		
		try {
			Class.forName(dbClassName).newInstance();
			conn = DriverManager.getConnection(dbUrl);
			conn.setAutoCommit(true);
		} catch (Exception ee) {
			ee.printStackTrace();
		}

		if (conn == null) {
			System.err.println("警告: DbConnectionManager.getConnection() 获得数据库链接失?.\r\n\r\n链接类型:" + dbClassName + "\r\n链接位置:" + dbUrl);
		}
		return conn;
	}

	/*
	 * 功能：执行查询语 
	 */
	public ResultSet executeQuery(String sql, int maxCount) {
		try {
			conn = getConnection();
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			if (maxCount > 0) {
				stmt.setMaxRows(maxCount);
			}

			rs = stmt.executeQuery(sql);
		} catch (SQLException ex) {
			ex.printStackTrace();
			System.err.println(ex.getMessage());
		}
		return rs;
	}

	/*
	 * 功能:执行更新操作
	 */
	public int executeUpdate(String sql) {
		int result = 0;
		try {
			conn = getConnection();  
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE );
			result = stmt.executeUpdate(sql); //执行更新操作
		} catch (SQLException ex) {
			ex.printStackTrace();
			result = 0;
		}
		return result;
	}
	
	/*
	 * 功能:执行更新操作
	 */
	public int executeUpdate2(String sql, Object ... objs) {
		int result = 0;
		try {
			conn = getConnection();  
			ps = conn.prepareStatement(sql);
			int index = 1;
			for(Object obj : objs) {
				ps.setObject(index++, obj);
			}
			
			result = ps.executeUpdate();
		} catch (SQLException ex) {
			ex.printStackTrace();
			result = 0;
		}
		return result;
	}

	/*
	 * 功能:关闭数据库的连接
	 */
	public void close() {
		try {
			if (rs != null) {
				rs.close();
			}
			if (stmt != null) {
				stmt.close();
			}
			if (conn != null) {
				conn.close();
			}
			conn = null;
		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	public void commit() {
		
		try {
			getConnection().commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
