package copyDatabase;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Stack;

import com.mysql.jdbc.PreparedStatement;

public class MySQLConnectionDest {
	private static String classForName;
	private static String url;
	private static String user;
	private static String pass;
	private static Stack<Connection> connPools;

	static {
		connPools = new Stack<Connection>();

		classForName = "com.mysql.jdbc.Driver";
//		url = "jdbc:mysql://127.0.0.1:3306/datawavehouse";
//		url = "jdbc:mysql://localhost:3306/data?useUnicode=yes&characterEncoding=UTF-8";
//		username = "root";
//		password = "";

		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection connect() throws SQLException {
		Connection conn = MySQLConnectionSoure.getConnection();
		System.out.println("dsds");

		String sql = "select * from myconfig";
		PreparedStatement preparedStatement;
		preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();

		while (resultSet.next()) {
			url = resultSet.getString("destination");
			user = resultSet.getString("username_dest");
			pass = resultSet.getString("password_dest");
		}
		System.out.println(url + "\t" + user + "\t" + pass);

		if (connPools.empty()) {
			conn = DriverManager.getConnection(url, user, pass);
		} else {
			conn = connPools.pop();
		}
		return conn;
	}

	public static void disconnect(Connection conn) {
		if (conn != null) {
			connPools.push(conn);
		}
	}

	public static void rollbackQuietly(Connection conn) {
		try {
			conn.rollback();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws SQLException {
		connect();
	}

}
