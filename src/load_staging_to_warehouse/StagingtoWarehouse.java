<<<<<<< HEAD
package load_staging_to_warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

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
		String field = "";
		String datatype = "";

//		4. duyệt từng dòng trong ResultSet
		while (rscontroll.next()) {
			field = rscontroll.getString("field");
			nameTable = rscontroll.getString("name_table_warehouse");
			datatype = rscontroll.getString("type_warehouse");
		}

//		5. cắt field lấy từ databasecontroll xuống
		StringTokenizer strToField = new StringTokenizer(field, ",");
		String arrField[] = new String[11];
		int k = 0;
		System.out.println(strToField.countTokens());
		while (strToField.hasMoreElements()) {
			arrField[k] = strToField.nextToken();
			k++;
		}

//		6. cắt datatype lấy từ databasecontroll xuống
		StringTokenizer strToDataType = new StringTokenizer(datatype, ",");
		String arrDataType[] = new String[11];
		int l = 0;
		System.out.println(strToDataType.countTokens());
		while (strToDataType.hasMoreElements()) {
			arrDataType[l] = strToDataType.nextToken();
			l++;
		}

// 		7. kết nối data warehouse		
		Connection connWareHouse = new GetConnection().getConnection("warehouse");

//		8. Khởi tạo query tạo table trong datawarehouse
		String sqlDest = "CREATE TABLE " + nameTable + "( " + arrField[0] + " " + arrDataType[0]
				+ " Not null AUTO_INCREMENT , " + arrField[1] + " " + arrDataType[1] + ", " + arrField[2] + " "
				+ arrDataType[3] + "," + arrField[3] + " " + arrDataType[3] + "," + arrField[4] + " " + arrDataType[4]
				+ "," + arrField[5] + " " + arrDataType[5] + "," + arrField[6] + " " + arrDataType[6] + ","
				+ arrField[7] + " " + arrDataType[7] + "," + arrField[8] + " " + arrDataType[8] + "," + arrField[9]
				+ " " + arrDataType[9] + "," + arrField[10] + " " + arrDataType[10] + ", PRIMARY KEY (" + arrField[0]
				+ "))";

//		9. Thực hiện câu query tạo table warehouse
		PreparedStatement pSDataWH;
		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlDest);
		pSDataWH.execute();

//		10. tạo query insert into dữ liệu vào table dataware
		String sqlLoadWarehouse = "insert into " + nameTable + "(" + arrField[1] + "," + arrField[2] + "," + arrField[3]
				+ "," + arrField[4] + "," + arrField[5] + "," + arrField[6] + "," + arrField[7] + "," + arrField[8]
				+ "," + arrField[9] + "," + arrField[10] + ")" + " value(?,?,?,?,?,?,?,?,?,?)";
		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);

//		11. kết nối database staging
		Connection connStaging = new GetConnection().getConnection("staging");
		PreparedStatement pSStaging;

//		12. kết nối table my_logs trong data control
		String sqlcontrolllog = "select * from my_logs";
		PreparedStatement psControlllog = (PreparedStatement) connControll.prepareStatement(sqlcontrolllog);
		ResultSet rscontrolllog = psControlllog.executeQuery();

		int record_start = 0;
		int record_end = 0;
		int count_load = 0;
		int id = 0;
		int check_Warehouse = -1;
		while (rscontrolllog.next()) {
			id = rscontrolllog.getInt("id");
			if (rscontrolllog.getString("status_stagging").equals("OK Staging")) {
				record_end = rscontrolllog.getInt("record_end");
				count_load = rscontrolllog.getInt("load_row_stagging");

				String sqlstaging = "select * from users LIMIT " + record_start + " , " + record_end;
				System.out.println(sqlstaging);
				pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				ResultSet rsStaging = pSStaging.executeQuery();
				int count = 0;
				while (rsStaging.next()) {
					int mssv = rsStaging.getInt(arrField[1]);
					String ho_lot = rsStaging.getString((arrField[2]));
					String ten = rsStaging.getString((arrField[3]));
					String ngay_sinh = rsStaging.getString((arrField[4]));
					String ma_lop = rsStaging.getString((arrField[5]));
					String ten_lop = rsStaging.getString((arrField[6]));
					String dien_thoai = rsStaging.getString((arrField[7]));
					String email = rsStaging.getString((arrField[8]));
					String que_quan = rsStaging.getString((arrField[9]));
					String ghi_chu = rsStaging.getString((arrField[10]));
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
					pSDataWH.setInt(1, mssv);
					pSDataWH.setString(2, ho_lot);
					pSDataWH.setString(3, ten);
					pSDataWH.setString(4, ngay_sinh);
					pSDataWH.setString(5, ma_lop);
					pSDataWH.setString(6, ten_lop);
					pSDataWH.setString(7, dien_thoai);
					pSDataWH.setString(8, email);
					pSDataWH.setString(9, que_quan);
					pSDataWH.setString(10, ghi_chu);

					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				System.out.println(check_Warehouse + "dsd");
				if (count > 0) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'OK Warehouse', date_time_warehouse= now() where id=" + id;
					System.out.println(updateLog);
					psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
					psControlllog.executeUpdate();
				} else {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now() where id=" + id;
					System.out.println(updateLog);
					psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
					psControlllog.executeUpdate();
				}

				record_start = record_end;
				System.out.println(record_start);
				System.out.println(check_Warehouse);
				System.out.println(record_end);
			} else {
				String updateLog = "update my_logs set status_warehouse="
						+ " 'ERROR', date_time_warehouse= now() where id=" + id;
				System.out.println(updateLog);
				psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
				psControlllog.executeUpdate();
			}
		}
	}

	public static void loadDataStagingtoWarehouse() throws Exception {
//		1. kết nối data control
		Connection connControl = new GetConnection().getConnection("control");
		
	}

	public static void main(String[] args) throws Exception {
		try {
			LoadStagingtoWarehouse();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
=======
<<<<<<< HEAD
package load_staging_to_warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;

public class StagingtoWarehouse {
	public static void LoadStagingtoWarehouse() throws Exception {

//		1. Kết nối databasecontroll để lấy dữ liệu.
		Connection connControll = new GetConnection().getConnection("control");

//		2. Kết nối table my_configs
		String sqlcontroll = "select * from my_configs";
		PreparedStatement psControll = (PreparedStatement) connControll.prepareStatement(sqlcontroll);

//		3. trả về ResultSet 
		ResultSet rscontroll = psControll.executeQuery();

		String nameTable = "";
		String field = "";
		String datatype = "";

//		4. duyệt từng dòng trong ResultSet
		while (rscontroll.next()) {
			field = rscontroll.getString("field");
			nameTable = rscontroll.getString("name_table_warehouse");
			datatype = rscontroll.getString("type_warehouse");
		}

//		5. cắt field lấy từ databasecontroll xuống
		StringTokenizer strToField = new StringTokenizer(field, ",");
		String arrField[] = new String[11];
		int k = 0;
		System.out.println(strToField.countTokens());
		while (strToField.hasMoreElements()) {
			arrField[k] = strToField.nextToken();
			k++;
		}

//		6. cắt datatype lấy từ databasecontroll xuống
		StringTokenizer strToDataType = new StringTokenizer(datatype, ",");
		String arrDataType[] = new String[11];
		int l = 0;
		System.out.println(strToDataType.countTokens());
		while (strToDataType.hasMoreElements()) {
			arrDataType[l] = strToDataType.nextToken();
			l++;
		}

// 		7. kết nối data warehouse		
		Connection connWareHouse = new GetConnection().getConnection("warehouse");

//		8. Khởi tạo query tạo table trong datawarehouse
		String sqlDest = "CREATE TABLE " + nameTable + "( " + arrField[0] + " " + arrDataType[0]
				+ " Not null AUTO_INCREMENT , " + arrField[1] + " " + arrDataType[1] + ", " + arrField[2] + " "
				+ arrDataType[3] + "," + arrField[3] + " " + arrDataType[3] + "," + arrField[4] + " " + arrDataType[4]
				+ "," + arrField[5] + " " + arrDataType[5] + "," + arrField[6] + " " + arrDataType[6] + ","
				+ arrField[7] + " " + arrDataType[7] + "," + arrField[8] + " " + arrDataType[8] + "," + arrField[9]
				+ " " + arrDataType[9] + "," + arrField[10] + " " + arrDataType[10] + ", PRIMARY KEY (" + arrField[0]
				+ "))";

//		9. Thực hiện câu query tạo table trong warehouse
		PreparedStatement pSDataWH;
		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlDest);
		pSDataWH.execute();

//		10. tạo query insert into dữ liệu vào table datawarehouse
		String sqlLoadWarehouse = "insert into " + nameTable + "(" + arrField[1] + "," + arrField[2] + "," + arrField[3]
				+ "," + arrField[4] + "," + arrField[5] + "," + arrField[6] + "," + arrField[7] + "," + arrField[8]
				+ "," + arrField[9] + "," + arrField[10] + ")" + " value(?,?,?,?,?,?,?,?,?,?)";
		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);

//		11. kết nối database staging
		Connection connStaging = new GetConnection().getConnection("staging");
		PreparedStatement pSStaging;

//		12. kết nối table my_logs trong data control
		String sqlcontrolllog = "select * from my_logs";
		PreparedStatement psControlllog = (PreparedStatement) connControll.prepareStatement(sqlcontrolllog);
		ResultSet rscontrolllog = psControlllog.executeQuery();

		int record_start = 0;
		int record_end = 0;
		int count_load = 0;
		int id = 0;
		int check_Warehouse = -1;
		while (rscontrolllog.next()) {
			id = rscontrolllog.getInt("id");
			if (rscontrolllog.getString("status_stagging").equals("OK Staging")) {
				record_end = rscontrolllog.getInt("record_end");
				count_load = rscontrolllog.getInt("load_row_stagging");

				String sqlstaging = "select * from users LIMIT " + record_start + " , " + record_end;
				System.out.println(sqlstaging);
				pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				ResultSet rsStaging = pSStaging.executeQuery();
				int count = 0;
				while (rsStaging.next()) {
					int mssv = rsStaging.getInt(arrField[1]);
					String ho_lot = rsStaging.getString((arrField[2]));
					String ten = rsStaging.getString((arrField[3]));
					String ngay_sinh = rsStaging.getString((arrField[4]));
					String ma_lop = rsStaging.getString((arrField[5]));
					String ten_lop = rsStaging.getString((arrField[6]));
					String dien_thoai = rsStaging.getString((arrField[7]));
					String email = rsStaging.getString((arrField[8]));
					String que_quan = rsStaging.getString((arrField[9]));
					String ghi_chu = rsStaging.getString((arrField[10]));
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
					pSDataWH.setInt(1, mssv);
					pSDataWH.setString(2, ho_lot);
					pSDataWH.setString(3, ten);
					pSDataWH.setString(4, ngay_sinh);
					pSDataWH.setString(5, ma_lop);
					pSDataWH.setString(6, ten_lop);
					pSDataWH.setString(7, dien_thoai);
					pSDataWH.setString(8, email);
					pSDataWH.setString(9, que_quan);
					pSDataWH.setString(10, ghi_chu);

					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				System.out.println(check_Warehouse + "dsd");
				if (count > 0) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'OK Warehouse', date_time_warehouse= now() where id=" + id;
					System.out.println(updateLog);
					psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
					psControlllog.executeUpdate();
				} else {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now() where id=" + id;
					System.out.println(updateLog);
					psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
					psControlllog.executeUpdate();
				}

				record_start = record_end;
				System.out.println(record_start);
				System.out.println(check_Warehouse);
				System.out.println(record_end);
			} else {
				String updateLog = "update my_logs set status_warehouse="
						+ " 'ERROR', date_time_warehouse= now() where id=" + id;
				System.out.println(updateLog);
				psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
				psControlllog.executeUpdate();
			}
		}
	}

	public static void loadDataStagingtoWarehouse() throws Exception {
//		1. kết nối data control
		Connection connControl = new GetConnection().getConnection("control");
		
	}

	public static void main(String[] args) throws Exception {
		try {
			LoadStagingtoWarehouse();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
=======
package load_staging_to_warehouse;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.StringTokenizer;

import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;

public class StagingtoWarehouse {
	public static void LoadStagingtoWarehouse() throws Exception {
//		Connection connSoure = MySQLConnectionSoure.connect();

//		Kết nối databasecontroll để lấy dữ liệu
		Connection connControll = new GetConnection().getConnection("control");
//		

//		Kết nối table my_configs
		String sqlcontroll = "select * from my_configs";
		PreparedStatement psControll = (PreparedStatement) connControll.prepareStatement(sqlcontroll);
		ResultSet rscontroll = psControll.executeQuery();

		String nameTable = "";
		String field = "";
		String datatype = "";

		while (rscontroll.next()) {
			field = rscontroll.getString("field");
			nameTable = rscontroll.getString("name_table_warehouse");
			datatype = rscontroll.getString("type_warehouse");
		}
		System.out.println(nameTable);
		System.out.println(field);
		System.out.println(datatype);
//
//		cắt file lấy từ databasecontroll xuống
		StringTokenizer strToField = new StringTokenizer(field, ",");
		String arrField[] = new String[11];
		int k = 0;
		System.out.println(strToField.countTokens());
		while (strToField.hasMoreElements()) {
			arrField[k] = strToField.nextToken();
			k++;
		}
		System.out.println(arrField[10]);

//		cắt datatype lấy từ databasecontroll xuống
		StringTokenizer strToDataType = new StringTokenizer(datatype, ",");
		String arrDataType[] = new String[11];
		int l = 0;
		System.out.println(strToDataType.countTokens());
		while (strToDataType.hasMoreElements()) {
			arrDataType[l] = strToDataType.nextToken();
			l++;
		}
		System.out.println(arrDataType[1]);

		String sqlSoure = "SELECT * FROM " + nameTable;

//		Khởi tạo table trong datawarehouse
		String sqlDest = "CREATE TABLE " + nameTable + "( " + arrField[0] + " " + arrDataType[0]
				+ " Not null AUTO_INCREMENT , " + arrField[1] + " " + arrDataType[1] + ", " + arrField[2] + " "
				+ arrDataType[3] + "," + arrField[3] + " " + arrDataType[3] + "," + arrField[4] + " " + arrDataType[4]
				+ "," + arrField[5] + " " + arrDataType[5] + "," + arrField[6] + " " + arrDataType[6] + ","
				+ arrField[7] + " " + arrDataType[7] + "," + arrField[8] + " " + arrDataType[8] + "," + arrField[9]
				+ " " + arrDataType[9] + "," + arrField[10] + " " + arrDataType[10] + ", PRIMARY KEY (" + arrField[0]
				+ "))";

// kết nối datawarehouse		
		Connection connWareHouse = new GetConnection().getConnection("warehouse");
		Connection connStaging = new GetConnection().getConnection("staging");
		PreparedStatement pSDataWH;
		PreparedStatement pSStaging;
//		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlDest);
//		pSDataWH.execute();

		String sqlLoadWarehouse = "insert into " + nameTable + "(" + arrField[1] + "," + arrField[2] + "," + arrField[3]
				+ "," + arrField[4] + "," + arrField[5] + "," + arrField[6] + "," + arrField[7] + "," + arrField[8]
				+ "," + arrField[9] + "," + arrField[10] + ")" + " value(?,?,?,?,?,?,?,?,?,?)";
		pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);

//		kết nối table my_logs
		String sqlcontrolllog = "select * from my_logs";
		PreparedStatement psControlllog = (PreparedStatement) connControll.prepareStatement(sqlcontrolllog);
		ResultSet rscontrolllog = psControlllog.executeQuery();

		int record_start = 0;
		int record_end = 0;
		int count_load = 0;
		int id = 0;
		int check_Warehouse = -1;
		while (rscontrolllog.next()) {
			id = rscontrolllog.getInt("id");
			if (rscontrolllog.getString("status_stagging").equals("OK Staging")) {
				record_end = rscontrolllog.getInt("record_end");
				count_load = rscontrolllog.getInt("load_row_stagging");

				String sqlstaging = "select * from users LIMIT " + record_start + " , " + record_end;
				System.out.println(sqlstaging);
				pSStaging = (PreparedStatement) connStaging.prepareStatement(sqlstaging);
				ResultSet rsStaging = pSStaging.executeQuery();
				int count = 0;
				while (rsStaging.next()) {
					int mssv = rsStaging.getInt(arrField[1]);
					String ho_lot = rsStaging.getString((arrField[2]));
					String ten = rsStaging.getString((arrField[3]));
					String ngay_sinh = rsStaging.getString((arrField[4]));
					String ma_lop = rsStaging.getString((arrField[5]));
					String ten_lop = rsStaging.getString((arrField[6]));
					String dien_thoai = rsStaging.getString((arrField[7]));
					String email = rsStaging.getString((arrField[8]));
					String que_quan = rsStaging.getString((arrField[9]));
					String ghi_chu = rsStaging.getString((arrField[10]));
					pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlLoadWarehouse);
					pSDataWH.setInt(1, mssv);
					pSDataWH.setString(2, ho_lot);
					pSDataWH.setString(3, ten);
					pSDataWH.setString(4, ngay_sinh);
					pSDataWH.setString(5, ma_lop);
					pSDataWH.setString(6, ten_lop);
					pSDataWH.setString(7, dien_thoai);
					pSDataWH.setString(8, email);
					pSDataWH.setString(9, que_quan);
					pSDataWH.setString(10, ghi_chu);

					if (pSDataWH.executeUpdate() == 1)
						count++;
				}
				System.out.println(check_Warehouse + "dsd");
				if (count > 0) {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'OK Warehouse', date_time_warehouse= now() where id=" + id;
					System.out.println(updateLog);
					psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
					psControlllog.executeUpdate();
				} else {
					String updateLog = "update my_logs set status_warehouse="
							+ " 'ERROR', date_time_warehouse= now() where id=" + id;
					System.out.println(updateLog);
					psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
					psControlllog.executeUpdate();
				}

				record_start = record_end;
				System.out.println(record_start);
				System.out.println(check_Warehouse);
				System.out.println(record_end);
			} else {
				String updateLog = "update my_logs set status_warehouse="
						+ " 'ERROR', date_time_warehouse= now() where id=" + id;
				System.out.println(updateLog);
				psControlllog = (PreparedStatement) connControll.prepareStatement(updateLog);
				psControlllog.executeUpdate();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		try {
			LoadStagingtoWarehouse();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
>>>>>>> e13eb67bf51a493cfb62163d914fb247419d5e07
