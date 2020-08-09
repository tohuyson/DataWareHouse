package com.datawarehouse.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.datawarehouse.connect.DBConnect;
import com.datawarehouse.model.Config;
import com.datawarehouse.model.Log;
import com.mysql.jdbc.Statement;

public class Management {
	Connection connection = null;
	PreparedStatement preparedStatement = null;
	ResultSet resultSet = null;

	private static ArrayList<Log> lstLog = new ArrayList<>();
	private static ArrayList<Config> lstConfig = new ArrayList<>();

	public Management() throws SQLException {
		connection = DBConnect.getJDBCConnection();
	}

	public static void main(String[] args) throws Exception {
		Management m = new Management();
//		Config c = new Config("", 3, "", "", "", "", "", "", 1, "", "", "", "");
//		System.out.println("------------");
		System.out.println(m.getLogs());
		System.out.println(m.filterList(1));
//		System.out.println(m.insert("", 3, "", "", "", "", "", "", 1, "", "", "", "","",""));
	}

	public List<Log> getLogs() throws SQLException {
		connection = DBConnect.getJDBCConnection();
		Log log = null;
		String sql = "select * from control.my_logs";
		try {
			lstLog = new ArrayList<Log>();
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();
			while (resultSet.next()) {
				log = new Log();
				log.setId(resultSet.getInt(1));
				log.setId_config(resultSet.getInt(2));
				log.setStatus_download(resultSet.getString(3));
				log.setDate_time_download(resultSet.getString(4));
				log.setLocal_path(resultSet.getString(5));
				log.setName_file_local(resultSet.getString(6));
				log.setExtension(resultSet.getString(7));
				log.setStatus_stagging(resultSet.getString(8));
				log.setDate_time_staging(resultSet.getString(9));
				log.setLoad_row_stagging(resultSet.getInt(10));
				log.setStatus_warehouse(resultSet.getString(11));
				log.setDate_time_warehouse(resultSet.getString(12));
				log.setLoad_row_warehouse(resultSet.getInt(13));
				log.setCreated_at(resultSet.getString(14));
				log.setUpdated_at(resultSet.getString(15));
				
				

				lstLog.add(log);
			}
//			preparedStatement.close();
//			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return lstLog;
	}

	public ArrayList<Log> filterList(int id_config) {
//		Connection connect_staging = DBConnect.getJDBCConnection();
		List<Log> list = null;
		Log log = null;
		try {
			list = new ArrayList<Log>();
			String sql = "select id_config, status_download, name_file_local, status_stagging, status_warehouse from my_logs where id_config='"+id_config+"'";
			preparedStatement = connection.prepareStatement(sql);
			resultSet = preparedStatement.executeQuery();

			while (resultSet.next()) {
				log = new Log();

				log.setId_config(resultSet.getInt("id_config"));
				log.setStatus_download(resultSet.getString("status_download"));
				log.setName_file_local(resultSet.getString("name_file_local"));
				log.setStatus_stagging(resultSet.getString("status_stagging"));
				log.setStatus_warehouse(resultSet.getString("status_warehouse"));
				
				list.add(log);
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		return (ArrayList<Log>) list;
	}

	public boolean insert(String hostname, int port, String username, String password, String remote_path,
			String local_path, String name_file, String name_table, int column_table, String field, String field_insert,
			String sql_create, String name_table_warehouse, String sql_insert_table, String field_convert) throws Exception {
		try {
			connection = DBConnect.getJDBCConnection();
			String query = " insert into my_configs(host_name,port,user_name,password,remote_path,local_path,name_file_type,"
					+ "name_table_staging,colum_table_staging, field, field_insert,sql_create_table, "
					+ "name_table_warehouse, sql_insert_table, field_convert)"
					+ " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

			// create the mysql insert preparedstatement
			PreparedStatement preparedStmt = connection.prepareStatement(query);
			preparedStmt.setString(1, hostname);
			preparedStmt.setInt(2, port);
			preparedStmt.setString(3, username);
			preparedStmt.setString(4, password);
			preparedStmt.setString(5, remote_path);
			preparedStmt.setString(6, local_path);
			preparedStmt.setString(7, name_file);
			preparedStmt.setString(8, name_file);
			preparedStmt.setInt(9, column_table);
			preparedStmt.setString(10, field);
			preparedStmt.setString(11, field_insert);
			preparedStmt.setString(12, sql_create);
			preparedStmt.setString(13, name_table_warehouse);
			preparedStmt.setString(14, sql_insert_table);
			preparedStmt.setString(15, field_convert);
			

			// execute the preparedstatement
			preparedStmt.execute();

		} catch (Exception e) {
			System.err.println(e.getMessage());
			return false;
		}
		return true;
	}
}
