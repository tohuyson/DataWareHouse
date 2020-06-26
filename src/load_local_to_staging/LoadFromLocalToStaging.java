package load_local_to_staging;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;

public class LoadFromLocalToStaging {
	public static void main(String[] args) throws Exception {
		new LoadFromLocalToStaging().staging("my_logs.status_download = 'OK Download'");
	}

	public void staging(String condition) throws Exception {
		Connection conn = null;
		PreparedStatement pre_control = null;
		try {
			// connect database control
			conn = new GetConnection().getConnection("control");
			// Tìm các file OK Download
			pre_control = (PreparedStatement) conn.prepareStatement(
					"SELECT my_logs.id ,my_logs.name_file_local, my_configs.name_table_staging ,my_configs.colum_table_staging, my_logs.local_path,my_logs.extension"
							+ " from my_logs JOIN my_configs on my_logs.id_config= my_configs.id" + " where " + condition);
			ResultSet re = pre_control.executeQuery();
			while (re.next()) {
				int id = re.getInt("id");
				String name_table_staging = re.getString("name_table_staging");
				String dir = re.getString("local_path");
				String filename = re.getString("name_file_local");
				String extend = re.getString("extension");
//				int number_column = re.getInt("colum_table_staging");

				// Kiểm tra file có tồn tại trên folder không
				String path = dir + filename + extend;
				System.out.println(path);
				File file = new File(path);
				if (!file.exists()) {
					System.out.println(path + "khong ton tai");
					String sql2 = "UPDATE my_logs SET "
							+ "my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now() WHERE id=" + id;
					pre_control = (PreparedStatement) conn.prepareStatement(sql2);
					pre_control.executeUpdate();
				} else {

					try {
						// Mở kết nối DB staging
						Connection conn_Staging = new GetConnection().getConnection("staging");
						PreparedStatement ps;
						int count = 0;
						List<Student> listStudent;
						if(extend.equals(".xlsx")) {
						// Mở file để đọc dữ liệu lên
						listStudent = new ReadFromExcelFile().readStudentsFromExcelFile(path);
						// insert tất cả students vào DB
						String sql = "INSERT INTO " + name_table_staging
								+ "(id,ma_sinhvien, ho_lot,ten, ngay_sinh,ma_lop,ten_lop,dien_thoai,email,que_quan,ghi_chu) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
						ps = (PreparedStatement) conn_Staging.prepareStatement(sql);
						listStudent.remove(0);
						for (Student student : listStudent) {
							ps.setString(1, student.getStt());
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
							// Lưu số dòng load thành công
							count += ps.executeUpdate();
						}
						}else if(extend.equals(".txt")) {
							System.out.println("file text");
						}
						else if(extend.equals(".csv")) {
							System.out.println("file csv");
						}
						System.out.println(
								"Load staging successfully:\t" + "file : " + filename + " ----> số dòng load thành công: " + count);
						// Kiểm tra số dòng đọc được vào staging của file
						if (count > 0) {
							String sql2 = "UPDATE my_logs SET load_row_stagging=" + count + ", "
									+ "status_stagging='OK Staging', my_logs.date_time_staging=now()  WHERE id=" + id;
							pre_control = (PreparedStatement) conn.prepareStatement(sql2);
							pre_control.executeUpdate();
						} else {

							String sql2 = "UPDATE my_logs SET my_logs.load_row_stagging =" + count + ", "
									+ " my_logs.status_stagging='ERROR Staging', my_logs.date_time_staging=now()  WHERE id="
									+ id;
							pre_control = (PreparedStatement) conn.prepareStatement(sql2);
							pre_control.executeUpdate();
						}

					} catch (IOException e) {
						e.printStackTrace();
					}
				}

			}
			// Đóng kết nối
			re.close();
			pre_control.close();
			conn.close();

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
