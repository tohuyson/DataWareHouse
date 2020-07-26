package load_staging_to_warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

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
		String sql = "SELECT * FROM my_logs JOIN my_configs ON my_logs.id_config= my_configs.id WHERE my_logs.id_config="
				+ config;
		PreparedStatement prS = (PreparedStatement) connControl.prepareStatement(sql);
		ResultSet rS = prS.executeQuery();

		String nameTable = "";
		int colum_table = -1;
		String field = "";

//		System.out.println(field);

		Connection connWareHouse = new GetConnection().getConnection("warehouse");
		PreparedStatement pSDataWH;
		int check_Warehouse = -1;

		while (rS.next()) {
			nameTable = rS.getString("name_table_staging");
			colum_table = rS.getInt("colum_table_staging");
			field = rS.getString("field");
			int id = rS.getInt("id");

			System.out.println(field);
			System.out.println(nameTable);

			String[] arrField = field.split("\\,");
			System.out.println(arrField.length);
			System.out.println(rS.getString("status_stagging"));
			if (rS.getString("status_stagging").equals("OK Staging")) {

				System.out.println("sss");
				Connection connStaging = new GetConnection().getConnection("staging");

				String sqlstaging = "select * from " + nameTable;
				PreparedStatement pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				int count = 0;
				ResultSet rsStaging = pSStaging.executeQuery();

				while (rsStaging.next()) {
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
//					pSDataWH.setInt(1, (Integer) null);
					for (int i = 1; i < arrField.length; i++) {
						System.out.println(i + "iiiii");
						String data = rsStaging.getString(i);
						System.out.println(data);
						try {
							int s = Integer.parseInt(data);
							System.out.println(s + "hfhfh");
							pSDataWH.setInt(i, s);
						} catch (NumberFormatException e) {
							LocalDate date = transform(1, data);
							if (date != null) {
								System.out.println(date.toString() + "ssdf");
								String sqldate = "select * from datadim where full_date='" + date + "'";
								System.out.println(sqldate);
								PreparedStatement datadim = (PreparedStatement) connWareHouse.prepareStatement(sqldate);
								ResultSet rsdim = datadim.executeQuery();
								int id_SK = -1;
								while (rsdim.next()) {
									id_SK = rsdim.getInt("id_SK");
								}
								pSDataWH.setInt(i, id_SK);
							} else {
								try {
									String s = String.valueOf(data);
									System.out.println("plpppllp");
									pSDataWH.setString(i, s);
								} catch (NullPointerException e2) {
									System.out.println("sds");
								}
							}
						}

					}

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

	public static LocalDate transform(int mode, String data) {
		switch (mode) {
		case 1:
			try {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
				LocalDate localDate = LocalDate.parse(data, formatter);
				return localDate;
			} catch (Exception e) {
				return transform(2, data);
			}
		case 2:
			try {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
				LocalDate localDate = LocalDate.parse(data, formatter);
				return localDate;
			} catch (Exception e) {
				return transform(3, data);
			}
		case 3:
			try {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("MM/dd/yyyy");
				LocalDate localDate = LocalDate.parse(data, formatter);
				return localDate;
			} catch (Exception e) {
				return transform(4, data);
			}
		case 4:
			try {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
				LocalDate localDate = LocalDate.parse(data, formatter);
				return localDate;
			} catch (Exception e) {
				return null;
			}
		default:
			return null;
		}
	}

	public static void main(String[] args) throws Exception {
		try {

//			LoadStagingtoWarehouse();
//			loadDataStagingtoWarehouse(
//					"INSERT INTO students (ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?)");
//			loadDataStagingtoWarehouseTranf(
//					"INSERT INTO students (stt,ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
//					1);
			loadDataStagingtoWarehouseTranf("INSERT INTO class (stt,ma_lop,ma_monhoc,nam_hoc) VALUES(?,?,?,?)", 3);

		} catch (SQLException e) {
			e.printStackTrace();
		}
//		System.out.println(transform(1,"1999/11/23"));
	}
}
