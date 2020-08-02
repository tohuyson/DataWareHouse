package load_warehouse_to_mart;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import connect.GetConnection;

public class WarehouseToMart {
	public static void main(String[] args) throws Exception {
		new WarehouseToMart().loadToMart();
	}
	
	public static void loadToMart() throws Exception {
		createTableDataMart();
		Connection conn = null;
		PreparedStatement ps = null;
		try {
			//1. ket noi toi warehouse
			Connection conn_DW = new GetConnection().getConnection("warehouse");
			//2. lay cac thuoc tinh trong table
			ps = conn_DW.prepareStatement("select * from warehouse.sinhvien");
			//3. tra ve RS chua cac record
			ResultSet rs = ps.executeQuery();
			
			if (rs.isBeforeFirst() == false) {
				System.out.println("KHONG CÓ DATA TRONG WAREHOUSE");
				System.exit(0);
			}
			while(rs.next()) {
				int id = rs.getInt(1);
				String ma_sinh_vien = rs.getString(2);
				String ho_lot = rs.getString(3);
				String ten = rs.getString(4);
				String ngay_sinh = rs.getString(5);
				String ma_lop = rs.getString(6);
				String ten_lop = rs.getString(7);
				String dien_thoai = rs.getString(8);
				String email = rs.getString(9);
				String que_quan = rs.getString(10);
				String ghi_chu = rs.getString(11);
				
				try {
					Connection conn_mart = new GetConnection().getConnection("mart");
					ps = conn_mart.prepareStatement("insert into sinhvien(id, ma_sinhvien, ho_lot, ten, ngay_sinh, ma_lop, ten_lop, "
							+ "dien_thoai, email, que_quan, ghi_chu) values "
							+ "(?,?,?,?,?,?,?,?,?,?,?)");
					ps.setInt(1, id);
					ps.setString(2, ma_sinh_vien);
					ps.setString(3, ho_lot);
					ps.setString(4, ten);
					ps.setString(5, ngay_sinh);
					ps.setString(6, ma_lop);
					ps.setString(7, ten_lop);
					ps.setString(8, dien_thoai);
					ps.setString(9, email);
					ps.setString(10, que_quan);
					ps.setString(11, ghi_chu);
					
					ps.executeUpdate();
					ps.close();

				}catch(SQLException e) {
					System.out.println(e.getMessage());
				}
			}

		}catch(SQLException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
		
		System.out.println("Finish load data from WAREHOUSE to DATAMART");
	}
	
	public static void createTableDataMart() throws Exception {
		Connection conn_mart = new GetConnection().getConnection("mart");
		Connection conn_DW = new GetConnection().getConnection("warehouse");
		try {
			String queryCreateTable = "CREATE TABLE IF NOT EXISTS mart.sinhvien SELECT *  FROM warehouse.sinhvien";
			conn_mart.prepareStatement(queryCreateTable).execute();
		} catch (SQLException e) {
			System.out.println("Create Table in DataMart: " + e.getMessage());
		}	
	}
}
