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

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;

public class LoadFromLocalToStaging {
	private BufferedReader bufferedReader;

	public static void main(String[] args) throws Exception {
		new LoadFromLocalToStaging().staging("my_logs.status_download = 'OK Download'");
		// new LoadFromLocalToStaging().readStudentsFromTXTOrCSV("E:\\23.
		// DataWarehouse\\baitap\\17130044_sang_nhom8.txt",
		// 11);
		// new LoadFromLocalToStaging().record_count();
	}

	public void staging(String condition) throws Exception {
		Connection conn = null;
		PreparedStatement pre_control = null;
		try {
			// connect database control
			conn = new GetConnection().getConnection("control");
			// Má»Ÿ káº¿t ná»‘i DB staging
			Connection conn_Staging = new GetConnection().getConnection("staging");
			// TÃ¬m cÃ¡c file OK Download
			pre_control = (PreparedStatement) conn.prepareStatement(
					"SELECT my_logs.id ,my_logs.name_file_local, my_configs.name_table_staging ,my_configs.colum_table_staging, my_logs.local_path,my_logs.extension,my_logs.status_stagging"
							+ " from my_logs JOIN my_configs on my_logs.id_config= my_configs.id" + " where "
							+ condition);
			ResultSet re = pre_control.executeQuery();
			while (re.next()) {
				int id = re.getInt("id");
				String name_table_staging = re.getString("name_table_staging");
				String dir = re.getString("local_path");
				String filename = re.getString("name_file_local");
				String extend = re.getString("extension");
				int number_column = re.getInt("colum_table_staging");
				String status_staging = re.getString("status_stagging");

				// Kiá»ƒm tra file cÃ³ tá»“n táº¡i trÃªn folder khÃ´ng
				String path = dir + filename + extend;
				System.out.println(path);
				File file = new File(path);
				if (!file.exists()) {
					System.out.println(path + " \tkhông tồn tại");
					String sql2 = "UPDATE my_logs SET "
							+ "my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now() WHERE id=" + id;
					pre_control = (PreparedStatement) conn.prepareStatement(sql2);
					pre_control.executeUpdate();
				} else if (status_staging.equals("OK Staging")) {
					System.out.println("File done load");
				} else {
					try {

						if (extend.equals(".osheet")) {
							System.out.println("bỏ qua");
							continue;
						}
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

						listStudent.remove(0);
						int stt = 0;
						int record = record_count();
						for (Student student : listStudent) {
							stt = Integer.parseInt(student.getStt());
							if (record == 0) {
								stt = record;
							} else {
								record++;
								stt = record;
							}
							ps.setString(1, String.valueOf(stt));
							ps.setString(2, student.getMasv());
							ps.setString(3, student.getHolot());
							ps.setString(4, student.getTen());
							ps.setString(5, student.getNgaysinh());
							ps.setString(6, student.getMalop());
							ps.setString(7, student.getTenlop());

							ps.setString(8, student.getDthoai());
							ps.setString(9, student.getEmail());
							ps.setString(10, student.getQuequan());
							ps.setString(11, student.getGhichu());
							ps.addBatch();
							// LÆ°u sá»‘ dÃ²ng load thÃ nh cÃ´ng
							count += ps.executeUpdate();
							stt++;
						}

						System.out.println("Load staging successfully:\t" + "file : " + filename
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
						if (count > 0) {
							sql2 = "UPDATE my_logs SET load_row_stagging=" + count + ", "
									+ "status_stagging='OK Staging', my_logs.date_time_staging=now()  WHERE id=" + id;
						} else {

							sql2 = "UPDATE my_logs SET my_logs.load_row_stagging =" + count + ", "
									+ " my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now()  WHERE id="
									+ id;
						}
						pre_control = (PreparedStatement) conn.prepareStatement(sql2);
						pre_control.executeUpdate();
						}
					} catch (IOException e) {
						throw new RemoteException(e.getMessage(), e);
					}
				}

			}
			// Ä�Ã³ng káº¿t ná»‘i
			re.close();
			pre_control.close();
			conn.close();

		} catch (

		SQLException e) {
			throw new RemoteException(e.getMessage(), e);
		}
	}

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
		try {
			bufferedReader = new BufferedReader(
					new InputStreamReader(new FileInputStream(path), Charset.forName("UTF-8")));
			String lineText;
			try {
				// bá»� header
				lineText = bufferedReader.readLine();
				while ((lineText = bufferedReader.readLine()) != null) {
					StringTokenizer tokenizer = new StringTokenizer(lineText, ",|");
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
					}
					listStudents.add(student);
					int c = data.length;
					// System.out.println("");
				}
			} catch (IOException e) {
				throw new RemoteException(e.getMessage(), e);
			}
		} catch (FileNotFoundException e) {
			throw new RemoteException(e.getMessage(), e);
		}
		return listStudents;
	}

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

	private Object getCellValue(Cell cell) {
		switch (cell.getCellType()) {
		case STRING:
			return cell.getStringCellValue();
		case NUMERIC:
			return (int) cell.getNumericCellValue();
		default:
			cell.getStringCellValue();
			break;
		}
		return cell.getStringCellValue();
	}

	private Workbook getWorkbook(FileInputStream inputStream, String excelFilePath) throws IOException {
		Workbook workbook = null;
		if (excelFilePath.endsWith("xlsx")) {
			workbook = new XSSFWorkbook(inputStream);
		} else if (excelFilePath.endsWith("xls")) {
			workbook = new HSSFWorkbook(inputStream);
		} else {
			throw new IllegalArgumentException("The specified file is not Excel file");
		}
		return workbook;
	}
}
