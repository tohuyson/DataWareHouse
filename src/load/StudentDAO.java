package load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import load_data_from_local_to_staging.GetConnection;

public class StudentDAO {

	private Connection conn;
	private PreparedStatement ps;

	public void insertListStudents(List<Student> listStudent) {
		try {
			conn = GetConnection.connect();
			String name = GetConnection.getNameDB();
			System.out.println(name);

			conn.setAutoCommit(false);
			String s = "CREATE TABLE students" 
					+ "(stt varchar(255),masv varchar(255), holot varchar(255),ten varchar(255), ngaysinh varchar(255),malop varchar(255),"
					+ "tenlop varchar(255), dien_thoai varchar(255),email varchar(255),quequan varchar(255),ghichu varchar(255))";
			String sql = "INSERT INTO students"
					+ "(stt,masv, holot,ten, ngaysinh,malop,tenlop,dien_thoai,email,quequan,ghichu) VALUES (?,?,?,?,?,?,?,?,?,?,?)";
			ps = conn.prepareStatement(s);
			ps.execute();
			ps = conn.prepareStatement(sql);
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
			}

			ps.executeBatch();

			// Gọi commit() để commit giao dịch với DB
			conn.commit();

			System.out.println("Record is inserted into Student table!");

		} catch (Exception e) {

			e.printStackTrace();
			MySQLConnectionUtils.rollbackQuietly(conn);

		} finally {

			try {
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}

			MySQLConnectionUtils.disconnect(conn);
		}

	}
}