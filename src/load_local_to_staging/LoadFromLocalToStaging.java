package load_local_to_staging;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.poi.openxml4j.exceptions.InvalidFormatException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;

public class LoadFromLocalToStaging {
	private BufferedReader bufferedReader;

	public static void main(String[] args) throws Exception {
		new LoadFromLocalToStaging().staging("my_logs.status_download = 'OK Download'");
		// new LoadFromLocalToStaging().readStudentsFromFile(new
		// File("D:\\Data\\17130044_sang_nhom8.txt"),11);

	}

	public void staging(String condition) throws Exception {
		Connection connect_control = null;
		PreparedStatement pre_control = null;
		String sql_update;
		String sql_insert;
		PreparedStatement pre_staging = null;
		int count = 0; 

		try {
			// 1.connect database control
			connect_control = new GetConnection().getConnection("control");
			// 2.connect database DB staging
			Connection conn_Staging = new GetConnection().getConnection("staging");
			// 3. Kiểm tra các file OK Download
			pre_control = (PreparedStatement) connect_control.prepareStatement(
					"SELECT my_logs.id ,my_logs.name_file_local, my_configs.name_table_staging, my_configs.field ,my_configs.colum_table_staging, my_logs.local_path,my_logs.extension,my_logs.status_stagging,my_logs.status_warehouse"
							+ " from my_logs JOIN my_configs on my_logs.id_config= my_configs.id" + " where "
							+ condition);
			// 4. Nhận ResultSet thỏa điều kiện
			ResultSet re = pre_control.executeQuery();
			// 5. Chạy từng record
			while (re.next()) {
				int id = re.getInt("id");
				String dir = re.getString("local_path");
				String filename = re.getString("name_file_local");
				String extend = re.getString("extension");
				String name_table_staging = re.getString("name_table_staging");
				int number_column = re.getInt("colum_table_staging");
				String status_staging = re.getString("status_stagging");
				String status_warehouse = re.getString("status_warehouse");
				String fields = re.getString("field");

				// 6. Kiểm tra file có tồn tại trong folder hay không
				String path = dir + filename + extend;
				System.out.println(path);
				File file = new File(path);
				// nếu không tồn tại
				if (!file.exists()) {
					// 6.1: thông báo file không tồn tại
					System.out.println(file + " \tkhông tồn tại");
					// 6.1.1: cập nhật xuống database
					sql_update = "UPDATE my_logs SET "
							+ "my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now() WHERE id=" + id;
					pre_control = (PreparedStatement) connect_control.prepareStatement(sql_update);
					pre_control.executeUpdate();
				} else
				// nếu tồn tại
				// 7. kiểm tra dữ liệu đã load lên warehouse chưa thì xóa hết dữ liệu
				if (status_warehouse.equals("OK Warehouse")) {
					String sql_truncate = "TRUNCATE TABLE " + name_table_staging;
					pre_control = (PreparedStatement) conn_Staging.prepareStatement(sql_truncate);
					pre_control.executeUpdate();
					System.out.println("File done load to warehouse");

				} else
				// 8. kiểm tra file đã được load vào staging chưa
				if (status_staging.equals("OK Staging")) {
					System.out.println("File done load to staging");
				} else {
					try {

						// chạy thêm for field
						System.out.println(" Fields: " + fields);

						String temp = "(" + fields.replace("id,", "") + ")";
						System.out.println("Temp       " + temp);

						// 9. Kiểm tra loại file
						// 9.1. Nếu là file đuôi osheet thì bỏ qa không đọc
						if (extend.equals(".osheet")) {
							System.out.println("bỏ qua");
							continue;
						}
						//////////////////////////////////////////// main//////////////////////////////////////////////////////////
						String listStudent = "";
						System.out.println("===========================================");
						// 9.2: Nếu là file excel
						if (extend.equals(".xlsx")) {
							System.out.println("Start loading file excel............");
							// Load dữ liệu kiểu excel
							 listStudent = loadingExcel(path, number_column);

						} else
						// 9.3 :Nếu là file txt hoặc csv
						if (extend.equals(".txt") || extend.equals(".csv")) {
							System.out.println("Start ............");
							// Load dữ liệu kiểu txt,csv
							listStudent = readStudentsFromFile(file, number_column);
						}

						// 10. kiểm tra dữ liệu load từ file có record thỏa mãn
						if (!listStudent.isEmpty()) {
							// 11. insert tất cả các student vào bảng staging

							sql_insert = "INSERT INTO " + name_table_staging + temp + " VALUES " + listStudent;
							pre_staging = (PreparedStatement) conn_Staging.prepareStatement(sql_insert);
							// 12. Đếm số dòng load thành công, thông báo ra màn hình
							count += pre_staging.executeUpdate();
						}

						System.out.println("Load staging successfully:\t" + "file : " + filename
								+ " ----> Số dòng load thành công: " + count);
						// String sql_update;
						if (count > 0) {
							// 12.1: update trạng thái load thành công, thời gian load và số dòng đã load
							sql_update = "UPDATE my_logs SET load_row_stagging=" + count + ", "
									+ "status_stagging='OK Staging', my_logs.date_time_staging=now()  WHERE id=" + id;
						} else {
							// 12.2. update trạng thái load thất bại, thời gian load và số dòng đã load=0
							sql_update = "UPDATE my_logs SET my_logs.load_row_stagging =" + count + ", "
									+ " my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now()  WHERE id="
									+ id;
						}
						pre_control = (PreparedStatement) connect_control.prepareStatement(sql_update);
						pre_control.executeUpdate();
						// }
					} catch (IOException e) {
						throw new RemoteException(e.getMessage(), e);
					}
				}

			}
			// 13. Đóng kết nối
			re.close();
			pre_control.close();
			connect_control.close();

		} catch (SQLException e) {
			throw new RemoteException(e.getMessage(), e);
		}
	}

	public String readStudentsFromFile(File file, int number_column) throws RemoteException {
		String listStudents = "";
		try {
			// 9.3.1: Mở file đọc dữ liệu kèm định dạng Charset UTF-8
			bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
			try {
				// 9.3.2: Đọc bỏ header
				System.out.println("Header:" + bufferedReader.readLine());
				String lineText = bufferedReader.readLine();
				// 9.3.3: Đọc từng dòng trong file
				while (lineText != null) {
					// 9.3.4: Cắt từng thuộc tính và đếm tổng thuộc tính trong từng dòng
					StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
					System.out.println(" Số value_column read: " + tokenizer.countTokens());
					if (tokenizer.countTokens() == number_column - 1) {
						listStudents += "('" + tokenizer.nextToken() + "'";
						while (tokenizer.hasMoreTokens()) {
							// 9.3.5: lấy giá trị tại từng cột của hàng được chỉ định theo định dạng sql để
							// insert
							listStudents += ", N'" + tokenizer.nextToken() + "'";
						}
						listStudents += "), ";
					}
					lineText = bufferedReader.readLine();
					System.out.println("Student: " + lineText);
				}
				// 9.3.6: Kiểm tra dữ liệu sinh viên
				if (listStudents.isEmpty()) {
					System.out.println("Dữ liệu rỗng");
					return "";
				} else {
					System.out.println("vào đây ");
					listStudents = listStudents.substring(0, listStudents.lastIndexOf(","));
					listStudents += ";";
					System.out.println("List ST: " + listStudents.toString());
				}
				// 9.3.7: Đóng kết nối
				bufferedReader.close();
			} catch (IOException e) {
				throw new RemoteException(e.getMessage(), e);
			}
		} catch (FileNotFoundException e) {
			throw new RemoteException(e.getMessage(), e);
		}
		return listStudents;
	}

	private String loadingExcel(String fileName, int number_column) throws InvalidFormatException, IOException {
		FileInputStream fileInStream = new FileInputStream(fileName);
		int sheetIdx = 0;
		// 9.2.1: Mở xlsx và lấy trang tính yêu cầu từ bảng tính
		XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
		XSSFSheet selSheet = workBook.getSheetAt(sheetIdx);

		// 9.2.2: Lặp qua tất cả các hàng trong trang tính đã chọn
		Iterator<Row> rowIterator = selSheet.iterator();
		List<String> listStudents = new ArrayList<String>();

		while (rowIterator.hasNext()) {
			int temp = 0;
			Row row = rowIterator.next();

			// 9.2.3: Lặp qua tất cả các cột trong hàng và xây dựng "," tách chuỗi

			Iterator<Cell> cellIterator = row.cellIterator();
//			 System.out.println(" count " +selSheet.getRow(1).getCell(0));
			if (selSheet.getRow(0).getLastCellNum() == number_column - 1) {
				String student_item = "("; 
				// System.out.println("row " + row);
				// while (cellIterator.hasNext()) {
				while (temp < number_column-1 ) {
					temp++;
					if (cellIterator.hasNext()) {

						Cell cell = cellIterator.next();
						switch (cell.getCellType()) {

						case STRING:
							String value = "";
							value = cell.getStringCellValue().replaceAll("'", "");
							// System.out.println(value);
							student_item += "N'" + value + "'";
							break;
						case NUMERIC:
							student_item += "'" + cell.getNumericCellValue() + "'";
							break;
						case BOOLEAN:
							student_item += "N'" + cell.getBooleanCellValue() + "'";
							break;

						default:
							student_item += "'-1'";
							break;
						}
						if (cell.getColumnIndex() == number_column - 2) {
							// bỏ dấu phẩy cuối
						} else
							student_item += ",";
					} else
						student_item += "'-1'";
				}
				student_item += ")\n";
				listStudents.add(student_item);
			}
		}
		// 9.2.4: Bỏ phần header
		listStudents.remove(0);
		// 9.2.5: Add tất cả sinh viên theo định dạng câu lệnh insert sql
		String sql_students = "";
		for (int i = 0; i < listStudents.size(); i++) {
			sql_students += listStudents.get(i) + ",";
		}
		sql_students = sql_students.substring(0, sql_students.lastIndexOf(","));
		sql_students += ";";

//		 System.out.println(sql_students);
		// 9.2.6: Đóng file
		workBook.close();
		System.out.println("List ST: " + sql_students);
		return sql_students;
	}
}