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
		String sqlcontroll = "select * from my_logs JOIN my_configs ON my_logs.id_config= my_configs.id where my_logs.id_config="
				+ idConfig;
		PreparedStatement psControll = (PreparedStatement) connControll.prepareStatement(sqlcontroll);

//		3. trả về ResultSet 
		ResultSet rscontroll = psControll.executeQuery();

		String nameTable = "";
		String sql_create_table = "";
		String sql_insert_table = "";
//		4. duyệt từng dòng trong ResultSet
		while (rscontroll.next()) {
//			5. lấy các thuộc tính từ database
			sql_create_table = rscontroll.getString("sql_create_table");
			nameTable = rscontroll.getString("name_table_warehouse");
			sql_insert_table = rscontroll.getString("sql_insert_table");
			System.out.println(sql_insert_table);
			int idlog = rscontroll.getInt("my_logs.id");

//	 		6. kết nối data warehouse		
			Connection connWareHouse = new GetConnection().getConnection("warehouse");

//			7. check table có tồn tại hay không
			PreparedStatement pSDataWH;
			DatabaseMetaData checkCreateTable = (DatabaseMetaData) connWareHouse.getMetaData();
			ResultSet table = checkCreateTable.getTables(null, null, nameTable, null);
			if (table.next()) {
			} else {
//			8. Thực hiện tạo table trong warehouse
				System.out.println(sql_create_table);
				pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sql_create_table);
				pSDataWH.execute();

			}
//			9.gọi phương thức staging chạy theo từng idLog và idConfig
			LoadFromLocalToStaging staging = new LoadFromLocalToStaging();
			staging.staging("OK Download", idConfig, idlog);
//			10. thực hiện load từ staging qua wavehouse theo từ idlog và idConfig
			loadDataStagingtoWarehouse(connControll, connWareHouse, sql_insert_table, idConfig, idlog);

		}
	}

	public static void loadDataStagingtoWarehouse(Connection connControl, Connection connWareHouse,
			String sqlLoadWarehouse, int config, int idLog) throws Exception {
//		1. thực hiện câu query
		String sql = "SELECT * FROM my_logs JOIN my_configs ON my_logs.id_config= my_configs.id WHERE my_logs.id_config="
				+ config + " and my_logs.id=" + idLog;
		PreparedStatement prS = (PreparedStatement) connControl.prepareStatement(sql);
//		2. trả về Result set
		ResultSet rS = prS.executeQuery();

		String nameTable = "";
		int colum_table = -1;
		String field = "";
		String status_warehouse = "";
		String field_convert = "";

		PreparedStatement pSDataWH;
// 		3. chạy từng dòng
		while (rS.next()) {
//			4. thực hiện lấy dữ liệu từ database
			nameTable = rS.getString("name_table_staging");
			colum_table = rS.getInt("colum_table_staging");
			field = rS.getString("field");
			int id = rS.getInt("id");
			status_warehouse = rS.getString("status_warehouse");
			field_convert = rS.getString("field_convert");
//			5. thực hiện cắt field đã lấy ra theo dấu phẩy 
			String[] arrField = field.split("\\,");
//			6. kiểm tra trạng thái status_stagging trong log
			if (rS.getString("status_stagging").equals("OK Staging")) {
//			7. kết nối database staging
				Connection connStaging = new GetConnection().getConnection("staging");
//				8. thực hiện câu query quét dữ liệu từ table trong staging
				String sqlstaging = "select * from " + nameTable;
				PreparedStatement pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				int count = 0;
//				9. trả về Result set
				ResultSet rsStaging = pSStaging.executeQuery();
//				10. chạy từng dòng
				while (rsStaging.next()) {
//					11. thực hiện câu query insert vào warehouse
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
//					12. thực hiện duyệt field cắt ở bước 5
					for (int i = 0; i < arrField.length; i++) {
						if (i >= 1) {
							String data = rsStaging.getString(i + 1);
//							13. kiểm tra có phải field cần chuyển đổi dữ liệu không
							if (arrField[i].equals(field_convert)) {
//								14. phương thức chuyển đổi idSk
								int idsk = convertIdSk(field_convert, connWareHouse, data);
								pSDataWH.setInt(i, idsk);
							} else {
//								15. đưa dữ liệu về đúng dạng
								try {
									int s = Integer.parseInt(data);
									pSDataWH.setInt(i, s);
								} catch (NumberFormatException e) {

									try {
										String s = String.valueOf(data);
										pSDataWH.setString(i, s);
									} catch (NullPointerException e2) {
									}

								}
							}
						}

					}
//					16. thực hiện insert vào warehouse
					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				if (count > 0) {
//					17. thực hiện update nếu insert thành công
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
//					18. thực hiện update nếu insert lỗi
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now(), load_row_warehouse='-1' where id=" + id;
					prS = (PreparedStatement) connControl.prepareStatement(updateLog);
					prS.executeUpdate();
					System.out.println("Load warehouse không thành công");
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
			int id_SK = 1;
			if (date != null) {
				String sqldate = "select * from datadim where full_date='" + date + "'";
				PreparedStatement datadim = (PreparedStatement) connWareHouse.prepareStatement(sqldate);
				ResultSet rsdim = datadim.executeQuery();

				while (rsdim.next()) {
					if (rsdim.getInt("id_SK") != -1) {
						id_SK = rsdim.getInt("id_SK");
					} else {
						id_SK = 1;
					}
				}
			} else {
				id_SK = 1;
			}
			return id_SK;
		case "active":
			Connection connStaging = new GetConnection().getConnection("staging");
			String sqldate = "select id,stt,ma_monhoc,ten_monhoc,tin_chi,khoa_quanly from subjects where active=1";
			PreparedStatement datasubject = (PreparedStatement) connWareHouse.prepareStatement(sqldate);
			ResultSet rssubject = datasubject.executeQuery();
			while (rssubject.next()) {

				if (rssubject.getRow() != 0) {
					int id = rssubject.getInt("id");
					int stt = rssubject.getInt("stt");
					String ma_monhoc = rssubject.getString("ma_monhoc");
					String ten_monhoc = rssubject.getString("ten_monhoc");
					int tin_chi = rssubject.getInt("tin_chi");
					String khoa_quanly = rssubject.getString("khoa_quanly");
					String sql = "select stt,ma_monhoc,ten_monhoc,tin_chi,khoa_quanly from subjects where active=0 and stt="
							+ stt;
					PreparedStatement prStagging = (PreparedStatement) connStaging.prepareStatement(sql);
					ResultSet rsStagging = prStagging.executeQuery();

					while (rsStagging.next()) {
						if (stt == (rsStagging.getInt("stt"))) {
							if (!ten_monhoc.equals(rsStagging.getString("ten_monhoc"))
									|| tin_chi != rsStagging.getInt("tin_chi")
									|| !khoa_quanly.equals(rsStagging.getString("khoa_quanly"))) {
								String sqlupdate = "update subjects set active=0 where id=" + id;
								PreparedStatement psupdate = (PreparedStatement) connWareHouse
										.prepareStatement(sqlupdate);
								psupdate.executeUpdate();
							} else {
							}
						} else {
						}
					}
				}
			}
			break;
		case "ma_monhoc":
			String sqlSubject = "select * from subjects where active=1 and ma_monhoc=" + data;
			PreparedStatement dataSubject = (PreparedStatement) connWareHouse.prepareStatement(sqlSubject);
			ResultSet rsSubject = dataSubject.executeQuery();
			int id = 1;
			while (rsSubject.next()) {
				id = rsSubject.getInt("id");

			}
			return id;
		}

		return 1;
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
		try {

//		System.out.println(convertIdSk("ngay_sinh", "23/11/1999"));

			LoadStagingtoWarehouse(4);

//			loadDataStagingtoWarehouse(
//					"INSERT INTO students (ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?)");
//			loadDataStagingtoWarehouseTranf(
//					"INSERT INTO students (stt,ma_sv,ho_lot,ten,ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
//					1, 1);
//			loadDataStagingtoWarehouse("INSERT INTO  (stt,ma_monhoc,ten_monhoc,tin_chi,khoa_quanly,ghi_chu,active) VALUES(?,?,?,?,?,?,?)", 29, 2);

		} catch (SQLException e) {
			e.printStackTrace();
		}
//		System.out.println(transform(1,"20/02/1990"));
	}
}
