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
<<<<<<< HEAD
			// connect database control
			conn = new GetConnection().getConnection("control");
			// Má»Ÿ káº¿t ná»‘i DB staging
			Connection conn_Staging = new GetConnection().getConnection("staging");
			// TÃ¬m cÃ¡c file OK Download
			pre_control = (PreparedStatement) conn.prepareStatement(
					"SELECT my_logs.id ,my_logs.name_file_local, my_configs.name_table_staging ,my_configs.colum_table_staging, my_logs.local_path,my_logs.extension,my_logs.status_stagging"
=======
			// lấy ra các file có trạng thái OK download
			// 1.connect database control
			connect_control = new GetConnection().getConnection("control");
			// 2.connect database DB staging
			Connection conn_Staging = new GetConnection().getConnection("staging");
			// 3. Kiểm tra các file OK Download
			pre_control = (PreparedStatement) connect_control.prepareStatement(
					"SELECT my_logs.id ,my_logs.name_file_local, my_configs.name_table_staging ,my_configs.colum_table_staging, my_logs.local_path,my_logs.extension,my_logs.status_stagging,my_logs.status_warehouse"
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
							+ " from my_logs JOIN my_configs on my_logs.id_config= my_configs.id" + " where "
							+ condition);
			ResultSet re = pre_control.executeQuery();
			while (re.next()) {
				int id = re.getInt("id");
				String dir = re.getString("local_path");
				String filename = re.getString("name_file_local");
				String extend = re.getString("extension");
				String name_table_staging = re.getString("name_table_staging");
				int number_column = re.getInt("colum_table_staging");
				String status_staging = re.getString("status_stagging");
				String status_warehouse = re.getString("status_warehouse");

				// load dữ liệu
				// 4. Kiểm tra file có tồn tại trong folder hay không
				String path = dir + filename + extend;
				System.out.println(path);
				File file = new File(path);
				if (!file.exists()) {
<<<<<<< HEAD
					System.out.println(path + " \tkhông tồn tại");
					String sql2 = "UPDATE my_logs SET "
=======
					// thông báo file không tồn tại, cập nhật xuống database
					System.out.println(file + " \tkhông tồn tại");
					sql_update = "UPDATE my_logs SET "
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
							+ "my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now() WHERE id=" + id;
					pre_control = (PreparedStatement) connect_control.prepareStatement(sql_update);
					pre_control.executeUpdate();
					// nếu tồn tại
				} else
				// kiểm tra đã load lên warehouse chưa thì xóa hết dữ liệu
				if (status_warehouse.equals("OK Warehouse")) {
					String sql_truncate = "TRUNCATE TABLE " + name_table_staging;
					pre_control = (PreparedStatement) conn_Staging.prepareStatement(sql_truncate);
					pre_control.executeUpdate();
					System.out.println("File done load to warehouse");

				} else
				// kiểm tra file đã đk load chưa
				if (status_staging.equals("OK Staging")) {
					System.out.println("File done load to staging");
				} else {
					try {

						if (extend.equals(".osheet")) {
							System.out.println("bỏ qua");
							continue;
						}
<<<<<<< HEAD
						PreparedStatement ps;
						int count = 0;
						List<Student> listStudent = null;
						// kiểm tra file đó có đủ trường dl k
						if (excuteDataNotTrueFiled(path, number_column) && extend.equals(".xlsx")) {
							// Má»Ÿ file Ä‘á»ƒ Ä‘á»�c dá»¯ liá»‡u lÃªn
							System.out.println("Start............");
							listStudent = readStudentsFromExcelFile(path, number_column);
						} else if (excuteDataNotTrueFiled(path, number_column)
								&& (extend.equals(".txt") || extend.equals(".csv"))) {
							listStudent = readStudentsFromTXTOrCSV(path, number_column);
						}
						// kiem tra co student ms inssert
						if (listStudent==null) {
							break;
						}else {
						// insert táº¥t cáº£ students vÃ o DB
						String sql = "INSERT INTO " + name_table_staging
								+ "(id,ma_sinhvien, ho_lot,ten, ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
						ps = (PreparedStatement) conn_Staging.prepareStatement(sql);
=======
						//////////////////////////////////////////// main//////////////////////////////////////////////////////////
						String listStudent = "";
						System.out.println("===========================================");
						if (extend.equals(".xlsx")) {
							// 6. Mở file để lấy dữ liệu
							// file excel thì chuyển sang file csv
							System.out.println("Start loading file excel............");
							listStudent = loadingExcel(path, number_column);
						} else if (extend.equals(".txt") || extend.equals(".csv")) {
							System.out.println("Start ............");
							listStudent = readStudentsFromFile(file, number_column);
						}

						// có dl k???
						if (!listStudent.isEmpty()) {
							// insert tất cả các student vào bảng staging
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d

							sql_insert = "INSERT INTO " + name_table_staging + " VALUES " + listStudent;
							pre_staging = (PreparedStatement) conn_Staging.prepareStatement(sql_insert);
							// Lưu lại số dòng load thành công
							count += pre_staging.executeUpdate();
						}

						System.out.println("Load staging successfully:\t" + "file : " + filename
<<<<<<< HEAD
								+ " ----> sá»‘ dÃ²ng load thÃ nh cÃ´ng: " + count);
						String sql_update_record;
						if (record == 0) {
							System.out.println(" ----> dÃ²ng record_end: " + listStudent.size());
							sql_update_record = "UPDATE my_logs SET record_end=" + listStudent.size() + " WHERE id="
									+ id;
						} else {
							System.out.println(" ----> dÃ²ng record_end: " + record);
							sql_update_record = "UPDATE my_logs SET record_end=" + record + " WHERE id=" + id;
						}
						pre_control = (PreparedStatement) conn.prepareStatement(sql_update_record);
						pre_control.executeUpdate();
						// Kiá»ƒm tra sá»‘ dÃ²ng Ä‘á»�c Ä‘Æ°á»£c vÃ o staging cá»§a file
						String sql2;
=======
								+ " ----> Số dòng load thành công: " + count);
						// String sql_update;
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
						if (count > 0) {
							// update trạng thái đã load
							sql_update = "UPDATE my_logs SET load_row_stagging=" + count + ", "
									+ "status_stagging='OK Staging', my_logs.date_time_staging=now()  WHERE id=" + id;
						} else {
							// k load
							sql_update = "UPDATE my_logs SET my_logs.load_row_stagging =" + count + ", "
									+ " my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now()  WHERE id="
									+ id;
						}
						pre_control = (PreparedStatement) connect_control.prepareStatement(sql_update);
						pre_control.executeUpdate();
<<<<<<< HEAD
						}
=======
						// }
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
					} catch (IOException e) {
						throw new RemoteException(e.getMessage(), e);
					}
				}

			}
			// Đóng kết nối
			re.close();
			pre_control.close();
			connect_control.close();

		} catch (

		SQLException e) {
			throw new RemoteException(e.getMessage(), e);
		}
	}

<<<<<<< HEAD
	private boolean excuteDataNotTrueFiled(String path, int number_column) {

		return true;
	}

	private int record_count() throws RemoteException {
		// Ä‘áº¿m tá»•ng record trong table staging Ä‘á»ƒ set id
		int record_count = -1;
		Connection conn_Staging;
		try {
			conn_Staging = new GetConnection().getConnection("staging");
			PreparedStatement ps;
			String sql_record = "SELECT COUNT(*) as a FROM users ";
			ps = (PreparedStatement) conn_Staging.prepareStatement(sql_record);
			ResultSet record = ps.executeQuery();
			while (record.next()) {
				record_count = record.getInt("a");
				System.out.println("record=" + record_count);
			}
		} catch (Exception e) {
			throw new RemoteException(e.getMessage(), e);
		}
		return record_count;

	}

	private List<Student> readStudentsFromTXTOrCSV(String path, int number_column) throws RemoteException {
		List<Student> listStudents = new ArrayList<Student>();
=======
	private String readStudentsFromFile(File file, int number_column) throws RemoteException {
		String listStudents = "";
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
		try {
			bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(file), Charset.forName("UTF-8")));
			try {
				// đọc bỏ header
				System.out.println("Header:" + bufferedReader.readLine());
				String lineText = bufferedReader.readLine();
				while (lineText != null) {
					StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
<<<<<<< HEAD
//					System.out.println(number_column);
					String[] data = new String[number_column];

					int k = 0;
					while (tokenizer.hasMoreElements()) {
						data[k] = tokenizer.nextToken();
						k++;
					}
					Student student = new Student();
					for (int i = 0; i < data.length; i++) {
						if (data[i] == null) {
							data[i] = "-1";
						} else
							// System.out.print(data[i] + "\t\t");
							student.setStt(data[0]);
						student.setMasv(data[1]);
						student.setHolot(data[2]);
						student.setTen(data[3]);
						student.setNgaysinh(data[4]);
						student.setMalop(data[5]);
						student.setTenlop(data[6]);
						student.setDthoai(data[7]);
						student.setEmail(data[8]);
						student.setQuequan(data[9]);
						student.setGhichu(data[10]);
=======
					System.out.println(" Số value_column read: " + tokenizer.countTokens());
					if (tokenizer.countTokens() == number_column) {
						listStudents += "('" + tokenizer.nextToken() + "'";
						while (tokenizer.hasMoreTokens()) {
							// lấy giá trị tại cột của hàng được chỉ định
							listStudents += ", N'" + tokenizer.nextToken() + "'";
						}
						listStudents += "), ";
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d
					}
					lineText = bufferedReader.readLine();
					System.out.println("Student: " + lineText);
				}
				if (listStudents.isEmpty()) {
					System.out.println("Dữ liệu rỗng");
					return "";
				} else {
					listStudents = listStudents.substring(0, listStudents.lastIndexOf(","));
					listStudents += ";";
					System.out.println(listStudents.toString());
				}
				bufferedReader.close();
			} catch (IOException e) {
				throw new RemoteException(e.getMessage(), e);
			}
		} catch (FileNotFoundException e) {
			throw new RemoteException(e.getMessage(), e);
		}
		return listStudents;
	}

<<<<<<< HEAD
	public List<Student> readStudentsFromExcelFile(String excelFilePath, int number_column) throws IOException {
		List<Student> listStudents = new ArrayList<Student>();
		FileInputStream inputStream = new FileInputStream(new File(excelFilePath));
		Workbook workBook = getWorkbook(inputStream, excelFilePath);
		Sheet firstSheet = workBook.getSheetAt(0);
		Iterator<Row> rows = firstSheet.iterator();
		while (rows.hasNext()) {
			Row row = rows.next();
			Iterator<Cell> cells = row.cellIterator();

			Student student = new Student();
			while (cells.hasNext()) {
				Cell cell = cells.next();
				int columnIndex = cell.getColumnIndex();
				System.out.println("cdddddd"+ columnIndex);
				int total_column = 0;
				if (!cells.hasNext()) {
					total_column = columnIndex+1;
					System.out.println("total column=" + total_column);
					if (total_column != number_column) {
						System.out.println("Số trường không trùng khớp !");
						return null;
					} else {
						switch (columnIndex) {
						case 0:
							student.setStt((String) String.valueOf(getCellValue(cell)));
							break;
						case 1:
							student.setMasv((String) String.valueOf(getCellValue(cell)));
							break;
						case 2:
							student.setHolot((String) getCellValue(cell));
							break;
						case 3:
							student.setTen((String) getCellValue(cell));
							break;
						case 4:
							student.setNgaysinh((String) String.valueOf(getCellValue(cell)));
							break;
						case 5:
							student.setMalop((String) getCellValue(cell));
							break;
						case 6:
							student.setTenlop((String) getCellValue(cell));
							break;

						case 7:
							student.setDthoai((String) String.valueOf(getCellValue(cell)));
							break;
						case 8:
							student.setEmail((String) getCellValue(cell));
							break;
						case 9:
							student.setQuequan((String) getCellValue(cell));
							break;
						case 10:
							student.setGhichu((String) getCellValue(cell));
							break;
						}
					}
					listStudents.add(student);
				}
//				if (columnIndex == number_column) {
//
//				} else {
//					System.out.println("Dữ liệu không trùng khớp");
//					return null;
//				}
			}
		}
		workBook.close();
		inputStream.close();
		return listStudents;
	}
=======
	private String loadingExcel(String fileName, int number_column) throws InvalidFormatException, IOException {
		FileInputStream fileInStream = new FileInputStream(fileName);
		int sheetIdx = 0;
		// Open the xlsx and get the requested sheet from the workbook
		XSSFWorkbook workBook = new XSSFWorkbook(fileInStream);
		XSSFSheet selSheet = workBook.getSheetAt(sheetIdx);

		// Iterate through all the rows in the selected sheet
		Iterator<Row> rowIterator = selSheet.iterator();
		List<String> listStudents = new ArrayList<String>();
		while (rowIterator.hasNext()) {

			Row row = rowIterator.next();

			// Iterate through all the columns in the row and build ","
			// separated string
			Iterator<Cell> cellIterator = row.cellIterator();
			// System.out.println(" count " +selSheet.getRow(0).getLastCellNum());
			if (selSheet.getRow(0).getLastCellNum() == number_column) {
				String student_item = "(";

				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();

					switch (cell.getCellType()) {
					case STRING:
						String value = "";
						value = cell.getStringCellValue().replaceAll("'", "");
						System.out.println(value);
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
					}
					if (cell.getColumnIndex() == number_column - 1) {
						// bỏ dấu phẩy cuối
					} else
						student_item += ",";
				}
				student_item += ")\n";
				listStudents.add(student_item);
>>>>>>> 3667bcc3ce9a1b2f506253f23a0e8537f12c337d

			}
		}
		listStudents.remove(0);
		String sql_students = "";
		for (int i = 0; i < listStudents.size(); i++) {
			sql_students += listStudents.get(i) + ",";
		}
		sql_students = sql_students.substring(0, sql_students.lastIndexOf(","));
		sql_students += ";";
		// System.out.println(sql_students);
		workBook.close();
		return sql_students;
	}
}