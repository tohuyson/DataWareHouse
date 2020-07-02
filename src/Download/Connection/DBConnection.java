package Connection;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import constants.Constants;

public class DBConnection extends Constants{
	public DBConnection() {
	} 

	public static Connection getJDBCConnection() {
		final String url = URL;  
		final String user = USER;
		final String password = PASSWORD; 
		try {
			Class.forName(DRIVER);
			try {
				return DriverManager.getConnection(url, user, password);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		return null;

	} 

	public static void main(String[] args) throws Exception {
		DBConnection database = new DBConnection();		
		Connection connection = DBConnection.getJDBCConnection();
		if (connection != null) {
			System.out.println("Connected!!!!");
		} else {
			System.out.println("Failed!!!!");
		}
		
		System.out.println(database.selectFromDatabase("select * from my_log"));
	}

	public static void doSQLScript(String sql) throws Exception {
		Connection connect = getJDBCConnection();
		Statement stmt = (Statement) connect.createStatement();
		stmt.executeUpdate(sql);
	}
 
	public ResultSet selectFromDatabase(String sql) throws Exception {
		Connection connect = getJDBCConnection();
		Statement stmt = (Statement) connect.createStatement();
		ResultSet rs = stmt.executeQuery(sql);
		return rs;
	}
	
}

