package load;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class StudentDAO {

	private Connection conn;
	private PreparedStatement ps;

	public void insertListBooks(List<Student> listStudent) {
		try {
			conn = MySQLConnectionUtils.connect();
			String name = MySQLConnectionUtils.getNameDB();
			System.out.println(name);
	
			conn.setAutoCommit(false);
			String s = "CREATE TABLE " + name+ "(Number varchar(255),Name varchar(255), Gender varchar(255),Identitycard varchar(255), Email varchar(255),Phone varchar(255),Address varchar(255))";
			String sql = "INSERT INTO " + name+ "(number,name, gender,identitycard, email,phone,address) VALUES (?,?,?,?,?,?,?)";
			ps = conn.prepareStatement(s);
			ps.execute();
			ps = conn.prepareStatement(sql);
			listStudent.remove(0);
			for (Student student : listStudent) {
				ps.setString(1, student.getNumber());
				ps.setString(2, student.getName());
				ps.setString(3, student.getGender());
				ps.setString(4, student.getIdentityCard());
				ps.setString(5, student.getEmail());
				ps.setString(6, student.getPhone());
				ps.setString(7, student.getAddress());

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