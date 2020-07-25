package load_staging_to_warehouse;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.StringTokenizer;

import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;
import load_local_to_staging.LoadFromLocalToStaging;

public class LoadDataDim {
	public static void loadDataDim() throws Exception {
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
		String localdatedim = "";

//		4. duyệt từng dòng trong ResultSet
		while (rscontroll.next()) {
			nameTable = rscontroll.getString("name_table_data_dim");
			field = rscontroll.getString("field_date_dim");
			datatype = rscontroll.getString("type_date_dim");
			localdatedim = rscontroll.getString("local_date_dim");
		}

//		5. cắt field lấy từ databasecontroll xuống
		StringTokenizer strToField = new StringTokenizer(field, ",");
		String arrField[] = new String[18];
		int k = 0;
		System.out.println(strToField.countTokens());
		while (strToField.hasMoreElements()) {
			arrField[k] = strToField.nextToken();
			k++;
		}

//		6. cắt datatype lấy từ databasecontroll xuống
		StringTokenizer strToDataType = new StringTokenizer(datatype, ",");
		String arrDataType[] = new String[18];
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
				+ " " + arrDataType[9] + "," + arrField[10] + " " + arrDataType[10] + "," + arrField[11] + " "
				+ arrDataType[11] + "," + arrField[12] + " " + arrDataType[12] + "," + arrField[13] + " "
				+ arrDataType[13] + "," + arrField[14] + " " + arrDataType[14] + "," + arrField[15] + " "
				+ arrDataType[15] + "," + arrField[16] + " " + arrDataType[16] + "," + arrField[17] + " "
				+ arrDataType[17] + ", PRIMARY KEY (" + arrField[0] + "))";
		System.out.println(sqlDest);
//		9. check table có tồn tại hay không
		PreparedStatement pSDataWH;
		DatabaseMetaData checkCreateTable = (DatabaseMetaData) connWareHouse.getMetaData();
		ResultSet table = checkCreateTable.getTables(null, null, nameTable, null);
		if (table.next()) {
			System.out.println("Table đã tồn tại");
		} else {
//		10. Thực hiện câu query tạo table warehouse
			pSDataWH = (PreparedStatement) connWareHouse.prepareStatement(sqlDest);
			pSDataWH.execute();

		}
//		String sqlLoaddatedim = "insert into " + nameTable + "(" + arrField[1] + "," + arrField[2] + "," + arrField[3]
//				+ "," + arrField[4] + "," + arrField[5] + "," + arrField[6] + "," + arrField[7] + "," + arrField[8]
//				+ "," + arrField[9] + "," + arrField[10] + "," + arrField[11] + "," + arrField[12] + "," + arrField[13]
//				+ "," + arrField[14] + "," + arrField[15] + "," + arrField[16] + "," + arrField[17] + ")"
//				+ " value(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
		LoadFromLocalToStaging staging = new LoadFromLocalToStaging();
		File file_date_dim = new File(localdatedim);
		String list = staging.readStudentsFromFile(file_date_dim, 18);
		String sql = "INSERT INTO " + nameTable + " VALUES " + list;
		PreparedStatement pre = (PreparedStatement) connWareHouse.prepareStatement(sql);
		pre.execute();
	}

	public static void main(String[] args) throws Exception {
		new LoadDataDim().loadDataDim();
	}
}
