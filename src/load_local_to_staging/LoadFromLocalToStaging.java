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
		LoadFromLocalToStaging loadFromLocalToStaging = new LoadFromLocalToStaging();

<<<<<<< HEAD
		loadFromLocalToStaging.staging("OK Download", Integer.parseInt(args[0]),29);
=======
		loadFromLocalToStaging.staging("OK Download", 1, 7);
>>>>>>> 209faad487b74946b1aeef4b50d6b55dbeb04215
	}

	public void staging(String condition, int id_config, int id_log) throws Exception {
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
					"SELECT my_logs.id ,my_logs.name_file_local, my_configs.name_table_staging, my_configs.field, my_configs.field_insert,my_configs.colum_table_staging, my_logs.local_path,my_logs.extension,my_logs.status_stagging,my_logs.status_warehouse"
							+ " from my_logs JOIN my_configs on my_logs.id_config= my_configs.id"
							+ " where my_logs.status_download = '" + condition + "'" + " AND my_logs.id_config="
							+ id_config + " AND my_logs.id=" + id_log);
			// 4. Nhận ResultSet với điều kiện
			ResultSet re = pre_control.executeQuery();
			// Kiểm tra nếu có result
			while (re.next()) {
				// 5. xử lý từng record
				int id = re.getInt("id");
				String dir = re.getString("local_path");
				String filename = re.getString("name_file_local");
				String extend = re.getString("extension");
				String name_table_staging = re.getString("name_table_staging");
				int number_column = re.getInt("colum_table_staging");
				String status_staging = re.getString("status_stagging");
				String status_warehouse = re.getString("status_warehouse");
				// String fields = re.getString("field");
				String field_insert = re.getString("field_insert");

				// 6. Kiểm tra file có tồn tại trong folder hay không
				String path = dir + filename + extend;
				System.out.println("*Loading File..................=> " + path);
				System.out.println();
				File file = new File(path);
				// nếu không tồn tại
				if (!file.exists()) {
					// 6.1: thông báo file không tồn tại
					System.out.println("\t\t" + file + " \tkhông tồn tại\n");
					// 6.1.1: cập nhật xuống database
					sql_update = "UPDATE my_logs SET "
							+ "my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now() WHERE id=" + id;
					pre_control = (PreparedStatement) connect_control.prepareStatement(sql_update);
					pre_control.executeUpdate();
				} else
				// nếu tồn tại
				
				// 7. kiểm tra file đã được load vào staging chưa
				if (status_staging.equals("OK Staging")) {
					// 7.1 nếu rồi thông báo ra màn hình đã load vào staging
					System.out.println("\t\t@_@ File done load to staging\n");
				} else {
					// chưa load vào staging
					try {

						// chạy thêm for field

						// 8. Kiểm tra định dạng file
					
						String listStudent = "";
						System.out.println("===========================================");
						//  Nếu là file excel
						if (extend.equals(".xlsx")) {
							System.out.println("\t+Start loading file excel............");
							// 8.1 Load dữ liệu kiểu excel
							listStudent = loadingExcel(path, number_column);

						} else
						//Nếu là file txt hoặc csv
						if (extend.equals(".txt") || extend.equals(".csv")) {
							// System.out.println("Start ............");
							//8.2 Load dữ liệu kiểu txt,csv
							listStudent = readStudentsFromFile(file, number_column);
						}

						// 9. kiểm tra dữ liệu đọc từ file 
						// nếu có
						if (!listStudent.isEmpty()) {
							// 9.1 insert tất cả các student vào bảng staging

							sql_insert = "INSERT INTO " + name_table_staging + field_insert + " VALUES " + listStudent;
							pre_staging = (PreparedStatement) conn_Staging.prepareStatement(sql_insert);
							
							count += pre_staging.executeUpdate();
						} else {
							// không có
							// 9.2 Thông báo dữ liệu rỗng
							System.out.println("\t\t\tDữ liệu rỗng!\n");
							count = 0;
							// continue;
						}
						
						// String sql_update;
						//10. Kiểm tra số dòng load vào table 
						if (count > 0) {
							// 10.1: update trạng thái load thành công, thời gian load và số dòng đã load
							sql_update = "UPDATE my_logs SET load_row_stagging=" + count + ", "
									+ "status_stagging='OK Staging', my_logs.date_time_staging=now()  WHERE id=" + id;
						} else {
							// 10.2. update trạng thái load thất bại, thời gian load và số dòng đã load=0
							sql_update = "UPDATE my_logs SET my_logs.load_row_stagging =" + count + ", "
									+ " my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now()  WHERE id="
									+ id;
						}
						pre_control = (PreparedStatement) connect_control.prepareStatement(sql_update);
						pre_control.executeUpdate();
						// }
						//11. thông báo số dòng load thành công ra màn hình
						System.out.println("\t\tLoad staging successfully:\t" + "file : " + filename
								+ " ----> Số dòng load thành công: " + count + "\n");
					} catch (IOException e) {
						throw new RemoteException(e.getMessage(), e);
					}
				}

			}
			// 4a. Đóng kết nối
			re.close();
			pre_control.close();
			connect_control.close();

		} catch (SQLException e) {
			e.printStackTrace();
			// System.out.println(e.getMessage());
			count = 0;
			System.out.println("Lỗi gì chưa xử lý được\n ");
			staging("ERROR Staging", id_config, id_log);
		}
	}

	public String readStudentsFromFile(File file, int number_column) throws RemoteException {
		String listStudents = "";
		try {
			// 8.2.1: Mở file đọc dữ liệu kèm định dạng Charset UTF-8
			bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
			try {
				// 8.2.2: Đọc bỏ header
				System.out.println("Header:" + bufferedReader.readLine());
				String lineText = bufferedReader.readLine();
				// 8.2.3: Đọc từng dòng trong file
				while (lineText != null) {
					// 8.2.4: Cắt từng thuộc tính và đếm tổng thuộc tính trong từng dòng
					StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
					// System.out.println(" Số value_column read: " + tokenizer.countTokens());
					if (tokenizer.countTokens() == number_column) {
						listStudents += "('" + tokenizer.nextToken() + "'";
						while (tokenizer.hasMoreTokens()) {
							// 8.2.5: lấy giá trị tại từng cột của hàng được chỉ định theo định dạng sql để
							// insert
							listStudents += ", N'" + tokenizer.nextToken() + "'";
						}
						listStudents += "), ";
					}
					lineText = bufferedReader.readLine();
					// System.out.println("Student: " + lineText);
				}
				// 8.2.6: Kiểm tra dữ liệu sinh viên
				if (listStudents.isEmpty()) {
					System.out.println("Dữ liệu rỗng");
					return "";
				} else {
					listStudents = listStudents.substring(0, listStudents.lastIndexOf(","));
					listStudents += ";";
					System.out.println("List ST: " + listStudents.toString());
				}
				// 8.2.7: Đóng kết nối
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
		// 8.1.1: Mở xlsx và lấy trang tính yêu cầu từ bảng tính
		XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
		XSSFSheet selSheet = workBook.getSheetAt(sheetIdx);

		int rowTotal = selSheet.getLastRowNum();

		System.out.println("total row " + rowTotal);
		// 8.1.2: Lặp qua tất cả các hàng trong trang tính đã chọn
		Iterator<Row> rowIterator = selSheet.iterator();
		List<String> listStudents = new ArrayList<String>();
		while (rowIterator.hasNext()) {
			int temp = 0;
			Row row = rowIterator.next();

			// 8.1.3: Lặp qua tất cả các cột trong hàng và xây dựng "," tách chuỗi

			Iterator<Cell> cellIterator = row.cellIterator();
			// xử lý cái file sinhvien_sang_nhom7 chơi trội.............
			if (selSheet.getRow(0) == null || selSheet.getRow(0).getLastCellNum() != number_column) {
				System.out.println("Làm data kiểu gì không hiểu");
				return "";
			}
			if (selSheet.getRow(0).getLastCellNum() == number_column) {
				String student_item = "(";
				while (temp < number_column) {
					temp++;
					if (cellIterator.hasNext()) {
						Cell cell = cellIterator.next();
						switch (cell.getCellType()) {

						case STRING:

							String value = "";
							value = cell.getStringCellValue().replaceAll("'", "");
							student_item += "N'" + value + "'";
							break;
						case NUMERIC:
							student_item += "'" + cell.getNumericCellValue() + "'";
							break;
						case BOOLEAN:
							student_item += "'-1'";
							break;
						case _NONE:
							student_item += "'-1'";
							break;
						case BLANK:
							student_item += "'-1'";
							break;
						case ERROR:
							student_item += "'-1'";
							break;
						case FORMULA:
							student_item += "'-1'";
							break;

						default:
							student_item += "'-1'";
							break;
						}
						if (cell.getColumnIndex() == number_column - 1) {
							// bỏ dấu phẩy cuối
						} else
							student_item += ",";
					} else

						student_item = "'-1'";
					// return "";
				}
				student_item += ")\n";
				listStudents.add(student_item);
			}
			// continue;
		}
		// 8.1.4: Bỏ phần header
		listStudents.remove(0);
		// 8.1.5: Add tất cả sinh viên theo định dạng câu lệnh insert sql
		String sql_students = "";
		a: for (int i = 0; i < listStudents.size(); i++) {
			String[] arr = listStudents.get(i).split(",'");
			// System.out.println(" vvo" +arr[0]);
			if (arr[0].contains("'-1'")) {
				listStudents.remove(listStudents.get(i));
				// continue a;
				break;
			} else {
				sql_students += listStudents.get(i) + ",";
			}
		}
		if (sql_students.isEmpty()) {
			return sql_students;
		}
		sql_students = sql_students.substring(0, sql_students.lastIndexOf(","));

		sql_students += ";";

		// System.out.println(sql_students);
		// 8.1.6: Đóng file
		workBook.close();
		// System.out.println("List ST: " + sql_students);
		return sql_students;
	}

}