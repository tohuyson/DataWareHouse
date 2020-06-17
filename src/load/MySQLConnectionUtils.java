package load;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Stack;

import com.mysql.jdbc.PreparedStatement;


public class MySQLConnectionUtils {
	private static String classForName;
	private static String url;
	private static String user;
	private static String pass;
	private static String nameDB;
	private static Stack<Connection> connPools;
	
	public static String getNameDB() {
		return nameDB;
	}

	public static void setNameDB(String nameDB) {
		MySQLConnectionUtils.nameDB = nameDB;
	}
	static {
		connPools = new Stack<Connection>();

		classForName = "com.mysql.jdbc.Driver";
////		url = "jdbc:mysql://127.0.0.1:3306/datawavehouse";
//		url = "jdbc:mysql://localhost:3306/datawavehouse?useUnicode=yes&characterEncoding=UTF-8";
//		username = "root";
//		password = "";

		try {
//			Class.forName(classForName);
//			Class.forName("com.mysql.cj.jdbc.Driver").newInstance();
			Class.forName("com.mysql.jdbc.Driver").newInstance();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getConnection() throws SQLException {
		Connection con = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/databasecontrol?useUnicode=yes&characterEncoding=UTF-8", "root", "");
		System.out.println("success");
		return con;
	}

	public static Connection connect() throws SQLException {
		Connection conn = getConnection();
		System.out.println("dsds");

		String sql = "select * from myconfig";
		PreparedStatement preparedStatement;
		preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
		ResultSet resultSet = preparedStatement.executeQuery();

		while (resultSet.next()) {
			nameDB = resultSet.getString("name");
			url = resultSet.getString("source");
			user = resultSet.getString("username_source");
			pass = resultSet.getString("password_source");
		}
		System.out.println(nameDB);
		System.out.println(url + "\t" + user + "\t" + pass);

		if (connPools.empty()) {
			conn = DriverManager.getConnection(url, user, pass);
			System.out.println("connect dbsource");
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
