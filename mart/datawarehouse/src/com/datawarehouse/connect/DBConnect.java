package com.datawarehouse.connect;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.datawarehouse.constants.Constants;

public class DBConnect extends Constants{
	
	public DBConnect() {
	}

	public static Connection getJDBCConnection() {
		final String url = URL;
		final String user = USER;
		final String password = PASSWORD;
		try {
			Class.forName(DRIVER);
			try {
				return DriverManager.getConnection(url, user, password);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;

	}

	public static void main(String[] args) throws Exception {
		DBConnect database = new DBConnect();
		Connection connection = database.getJDBCConnection();
		if (connection != null) {
			System.out.println("Connected!!!!");
		} else {
			System.out.println("Failed!!!!");
		}
		
	}

	public void doSQLScript(String sql) throws Exception {
		Connection connect = getJDBCConnection();
		Statement stmt = (Statement) connect.createStatement();
		stmt.executeUpdate(sql);
	}

	public ResultSet selectFromDatabase(String sql) throws Exception {
		Connection connect = getJDBCConnection();
		Statement stmt = (Statement) connect.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		return rs;
	}
	
	public void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
}

