package load_staging_to_warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;
import load_local_to_staging.LoadFromLocalToStaging;

public class StagingtoWarehouse {
	public static void LoadStagingtoWarehouse(int idConfig) throws Exception {

//		1. Kết nối databasecontroll để lấy dữ liệu
		Connection connControll = new GetConnection().getConnection("control");

//		2. Kết nối table my_configs
//		String sqlcontroll = "select * from my_configs where id=" + idConfig;
		String sqlcontroll = "select * from my_logs JOIN my_configs ON my_logs.id_config= my_configs.id where my_logs.id_config="
				+ idConfig;
		PreparedStatement psControll = (PreparedStatement) connControll.prepareStatement(sqlcontroll);

//		3. trả về ResultSet 
		ResultSet rscontroll = psControll.executeQuery();

//		12. load những dòng bên staging qua warehouse
		String nameTable = "";
		String sql_create_table = "";
		String sql_insert_table = "";
//		4. duyệt từng dòng trong ResultSet
		while (rscontroll.next()) {
			sql_create_table = rscontroll.getString("sql_create_table");
			nameTable = rscontroll.getString("name_table_warehouse");
			sql_insert_table = rscontroll.getString("sql_insert_table");
			int idlog = rscontroll.getInt("my_logs.id");

//	 		7. kết nối data warehouse		
			Connection connWareHouse = new GetConnection().getConnection("warehouse");
			Connection connStaging = new GetConnection().getConnection("staging");

//			9. check table có tồn tại hay không
			PreparedStatement pSDataWH;
			DatabaseMetaData checkCreateTable = (DatabaseMetaData) connWareHouse.getMetaData();
			ResultSet table = checkCreateTable.getTables(null, null, nameTable, null);
			if (table.next()) {
			} else {
//			10. Thực hiện câu query tạo table warehouse
				System.out.println(sql_create_table);
				pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sql_create_table);
				pSDataWH.execute();

			}
//			System.out.println(idConfig);
			LoadFromLocalToStaging staging = new LoadFromLocalToStaging();
			staging.staging("OK Download", idConfig, idlog);
			loadDataStagingtoWarehouse(sql_insert_table, idConfig, idlog);

		}
	}

	public static void loadDataStagingtoWarehouse(String sqlLoadWarehouse, int config, int idLog) throws Exception {
		Connection connControl = new GetConnection().getConnection("control");
		String sql = "SELECT * FROM my_logs JOIN my_configs ON my_logs.id_config= my_configs.id WHERE my_logs.id_config="
				+ config + " and my_logs.id=" + idLog;
		PreparedStatement prS = (PreparedStatement) connControl.prepareStatement(sql);
		ResultSet rS = prS.executeQuery();

		String nameTable = "";
		int colum_table = -1;
		String field = "";
		String status_warehouse = "";
		String field_convert = "";

//		System.out.println(field);

		Connection connWareHouse = new GetConnection().getConnection("warehouse");
		PreparedStatement pSDataWH;

		while (rS.next()) {
			nameTable = rS.getString("name_table_staging");
			colum_table = rS.getInt("colum_table_staging");
			field = rS.getString("field");
			int id = rS.getInt("id");
			status_warehouse = rS.getString("status_warehouse");
			field_convert = rS.getString("field_convert");
//			System.out.println(field_convert);

			String[] arrField = field.split("\\,");
//			System.out.println((arrField[0]));
			if (rS.getString("status_stagging").equals("OK Staging")) {

				Connection connStaging = new GetConnection().getConnection("staging");

				String sqlstaging = "select * from " + nameTable;
				PreparedStatement pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				int count = 0;
				ResultSet rsStaging = pSStaging.executeQuery();

				while (rsStaging.next()) {
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
					for (int i = 1; i <= arrField.length; i++) {
						if (i >= 2) {
							String data = rsStaging.getString(i);
							try {
								int s = Integer.parseInt(data);
								pSDataWH.setInt(i - 1, s);
							} catch (NumberFormatException e) {
								if (arrField[i - 1].equals(field_convert)) {
									int idsk = convertIdSk(field_convert, connWareHouse, data);
									pSDataWH.setInt(i - 1, idsk);
								} else {
									try {
										String s = String.valueOf(data);
										pSDataWH.setString(i - 1, s);
									} catch (NullPointerException e2) {
									}

								}
							}
						}
					}

					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				if (count > 0) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'OK Warehouse', date_time_warehouse= now(), load_row_warehouse=" + count + " where id="
							+ id;
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();

					System.out.println("Load warehouse thành công, rowload=" + count);

					String name_table_staging = rS.getString("name_table_staging");
					String sqlTruncate = "TRUNCATE TABLE " + name_table_staging;
					PreparedStatement psStaging = (PreparedStatement) connStaging.prepareStatement(sqlTruncate);
					psStaging.execute();
				} else if (!status_warehouse.equals("OK Warehouse")) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now(), load_row_warehouse='-1' where id=" + id;
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();
					System.out.println("Load warehouse không thành công");
//					continue;
				} else {
					System.out.println("File đã load rồi");
				}

			}

		}

	}

	public static int convertIdSk(String fieldConvert, Connection connWareHouse, String data) throws Exception {
		switch (fieldConvert) {
		case "ngay_sinh":
			LocalDate date = transform(1, data);
			int id_SK = 0;
			if (date != null) {
				String sqldate = "select * from datadim where full_date='" + date + "'";
				PreparedStatement datadim = (PreparedStatement) connWareHouse.prepareStatement(sqldate);
				ResultSet rsdim = datadim.executeQuery();

				while (rsdim.next()) {
//					id_SK = rsdim.getInt("id_SK");
					if (rsdim.getInt("id_SK") != -1) {
//						System.out.println("dsđsd");
						id_SK = rsdim.getInt("id_SK");
					} else {
						id_SK = 1;
					}
				}
			} else {
				id_SK = 1;
			}
//			break;
			System.out.println(id_SK);
			return id_SK;
		}
		return -1;
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
				return transform(5, data);
			}
		case 5:
			try {
				DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/M/yyyy");
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
//		try {

//		System.out.println(convertIdSk("ngay_sinh", "23/11/1999"));

//		LoadStagingtoWarehouse(2);

//			loadDataStagingtoWarehouse(
//					"INSERT INTO students (ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?)");
//			loadDataStagingtoWarehouseTranf(
//					"INSERT INTO students (stt,ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
//					1, 1);
//			loadDataStagingtoWarehouseTranf("INSERT INTO class (stt,ma_lop,ma_monhoc,nam_hoc) VALUES(?,?,?,?)", 3, 1);

//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		System.out.println(transform(1,"20/02/1990"));
	}
}
