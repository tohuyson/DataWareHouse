package load_staging_to_warehouse;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.joda.time.LocalDate;

import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;

public class StagingtoWarehouse {
	public static void LoadStagingtoWarehouse() throws Exception {

//		1. Kết nối databasecontroll để lấy dữ liệu
		Connection connControll = new GetConnection().getConnection("control");

//		2. Kết nối table my_configs
		String sqlcontroll = "select * from my_configs";
		PreparedStatement psControll = (PreparedStatement) connControll.prepareStatement(sqlcontroll);

//		3. trả về ResultSet 
		ResultSet rscontroll = psControll.executeQuery();

		String nameTable = "";
		String sql_create_table = "";
		String sql_insert_table = "";

//		4. duyệt từng dòng trong ResultSet
		while (rscontroll.next()) {
			sql_create_table = rscontroll.getString("sql_create_table");
			nameTable = rscontroll.getString("name_table_warehouse");
			sql_insert_table = rscontroll.getString("sql_insert_table");
		}

// 		7. kết nối data warehouse		
		Connection connWareHouse = new GetConnection().getConnection("warehouse");

//		9. check table có tồn tại hay không
		PreparedStatement pSDataWH;
		DatabaseMetaData checkCreateTable = (DatabaseMetaData) connWareHouse.getMetaData();
		ResultSet table = checkCreateTable.getTables(null, null, nameTable, null);
		if (table.next()) {
			System.out.println("Table đã tồn tại");
		} else {
//		10. Thực hiện câu query tạo table warehouse
			System.out.println(sql_create_table);
			pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sql_create_table);
			pSDataWH.execute();

		}

//		11. Lấy query insert into dữ liệu vào table dataware
//		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sql_insert_table);

//		12. load những dòng bên staging qua warehouse
		System.out.println(sql_insert_table);
		loadDataStagingtoWarehouse(sql_insert_table);

////	
	}

	public static void loadDataStagingtoWarehouse(String sqlLoadWarehouse) throws Exception {
		Connection connControl = new GetConnection().getConnection("control");
		String sql = "select * from my_logs ";
		PreparedStatement prS = (PreparedStatement) connControl.prepareStatement(sql);
		ResultSet rS = prS.executeQuery();

		Connection connWareHouse = new GetConnection().getConnection("warehouse");
		PreparedStatement pSDataWH;
		int check_Warehouse = -1;
		while (rS.next()) {
			int id = rS.getInt("id");
			if (rS.getString("status_stagging").equals("OK Staging")) {
				Connection connStaging = new GetConnection().getConnection("staging");
				String sqlstaging = "select * from users";
				PreparedStatement pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				int count = 0;
				ResultSet rsStaging = pSStaging.executeQuery();
				while (rsStaging.next()) {
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
					pSDataWH.setInt(1, rsStaging.getInt("ma_sinhvien"));
					pSDataWH.setString(2, rsStaging.getNString("ho_lot"));
					pSDataWH.setString(3, rsStaging.getNString("ten"));
					String ngay_sinh = rsStaging.getNString("ngay_sinh");
					System.out.println(ngay_sinh);
					String[] arrNgay_sinh = ngay_sinh.split("/");
					int id_SK = -1;
					if (arrNgay_sinh.length != 3) {
						continue;
					} else {
						LocalDate date = new LocalDate(Integer.parseInt(arrNgay_sinh[2]),
								Integer.parseInt(arrNgay_sinh[1]), Integer.parseInt(arrNgay_sinh[0]));

						System.out.println(date);
						String sqldate = "select * from datadim where full_date='" + date + "'";
						System.out.println(sqldate);
						PreparedStatement datadim = (PreparedStatement) connWareHouse.prepareStatement(sqldate);
						ResultSet rsdim = datadim.executeQuery();

						while (rsdim.next()) {
							id_SK = rsdim.getInt("id_SK");
						}
//					datadim.get
						System.out.println(id_SK);
					}

					pSDataWH.setInt(4, id_SK);
					pSDataWH.setString(5, rsStaging.getNString("ma_lop"));
					pSDataWH.setString(6, rsStaging.getNString("ten_lop"));
					pSDataWH.setString(7, rsStaging.getNString("dien_thoai"));
					pSDataWH.setString(8, rsStaging.getNString("email"));
					pSDataWH.setString(9, rsStaging.getNString("que_quan"));
					pSDataWH.setString(10, rsStaging.getNString("ghi_chu"));

					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				System.out.println(count);
				System.out.println(check_Warehouse + "dsd");
				if (count > 0) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'OK Warehouse', date_time_warehouse= now(), load_row_warehouse=" + count + " where id="
							+ id;
					System.out.println(updateLog);
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();
				} else {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now(), load_row_warehouse='-1' where id=" + id;
					System.out.println(updateLog);
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();
					continue;
				}
			}
		}
	}

	public static void loadDataStagingtoWarehouseTranf(String sqlLoadWarehouse, int config) throws Exception {
		Connection connControl = new GetConnection().getConnection("control");
		String sql = "select * from my_logs where id_config=" + config;
		PreparedStatement prS = (PreparedStatement) connControl.prepareStatement(sql);
		ResultSet rS = prS.executeQuery();

		String sqlConf = "select * from my_configs where id=" + config;
		PreparedStatement prSConf = (PreparedStatement) connControl.prepareStatement(sqlConf);
		ResultSet rSConf = prSConf.executeQuery();
		String nameTable = "";
		int colum_table = -1;
		while (rSConf.next()) {
			nameTable = rSConf.getString("name_table_staging");
			colum_table = rSConf.getInt("colum_table_staging");
		}
		System.out.println(colum_table);

		Connection connWareHouse = new GetConnection().getConnection("warehouse");
		PreparedStatement pSDataWH;
		int check_Warehouse = -1;
		while (rS.next()) {
			int id = rS.getInt("id");
			if (rS.getString("status_stagging").equals("OK Staging")) {
				Connection connStaging = new GetConnection().getConnection("staging");

				String sqlstaging = "select * from " + nameTable;
				PreparedStatement pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				int count = 0;
				ResultSet rsStaging = pSStaging.executeQuery();
				while (rsStaging.next()) {
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);

//					pSDataWH.setInt(1, rsStaging.getInt("ma_sinhvien"));
//					pSDataWH.setString(2, rsStaging.getNString("ho_lot"));
//					pSDataWH.setString(3, rsStaging.getNString("ten"));
//					String ngay_sinh = rsStaging.getNString("ngay_sinh");
//					System.out.println(ngay_sinh);
//					String[] arrNgay_sinh = ngay_sinh.split("/");
//					int id_SK = -1;
//					if (arrNgay_sinh.length != 3) {
//						continue;
//					} else {
//						LocalDate date = new LocalDate(Integer.parseInt(arrNgay_sinh[2]),
//								Integer.parseInt(arrNgay_sinh[1]), Integer.parseInt(arrNgay_sinh[0]));
//
//						System.out.println(date);
//						String sqldate = "select * from datadim where full_date='" + date + "'";
//						System.out.println(sqldate);
//						PreparedStatement datadim = (PreparedStatement) connWareHouse.prepareStatement(sqldate);
//						ResultSet rsdim = datadim.executeQuery();
//
//						while (rsdim.next()) {
//							id_SK = rsdim.getInt("id_SK");
//						}
////					datadim.get
//						System.out.println(id_SK);
//					}
//
//					pSDataWH.setInt(4, id_SK);
//					pSDataWH.setString(5, rsStaging.getNString("ma_lop"));
//					pSDataWH.setString(6, rsStaging.getNString("ten_lop"));
//					pSDataWH.setString(7, rsStaging.getNString("dien_thoai"));
//					pSDataWH.setString(8, rsStaging.getNString("email"));
//					pSDataWH.setString(9, rsStaging.getNString("que_quan"));
//					pSDataWH.setString(10, rsStaging.getNString("ghi_chu"));

					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				System.out.println(count);
				System.out.println(check_Warehouse + "dsd");
				if (count > 0) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'OK Warehouse', date_time_warehouse= now(), load_row_warehouse=" + count + " where id="
							+ id;
					System.out.println(updateLog);
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();
				} else {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now(), load_row_warehouse='-1' where id=" + id;
					System.out.println(updateLog);
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();
					continue;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		try {

//			LoadStagingtoWarehouse();
//			loadDataStagingtoWarehouse(
//					"INSERT INTO students (ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?)");
			loadDataStagingtoWarehouseTranf(
					"INSERT INTO students (ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?)",
					1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
