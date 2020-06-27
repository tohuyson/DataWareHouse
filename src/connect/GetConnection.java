package connect;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class GetConnection {

	String driver = null;
	String url = null;
	String user = null;
	String pass = null;
	String databasebName = null;
	
	public Connection getConnection(String location) throws Exception {
		String link = "config\\config.properties";
		Connection result = null;

		if (location.equalsIgnoreCase("control")) {
			try (InputStream input = new FileInputStream(link)) {
				Properties prop = new Properties();
				prop.load(input);
				url = prop.getProperty("url_local");
				databasebName = prop.getProperty("database_name_local");
				user = prop.getProperty("username_local");
				pass = prop.getProperty("pass_local");
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else if (location.equalsIgnoreCase("staging")) {
			try (InputStream input = new FileInputStream(link)) {
				Properties prop = new Properties();
				prop.load(input);
				driver = prop.getProperty("driver");
				url = prop.getProperty("url_staging");
				databasebName = prop.getProperty("database_name_staging");
				user = prop.getProperty("username_staging");
				pass = prop.getProperty("pass_staging");
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		} else {
			System.out.println(driver);

		}
//			Class.forName(driver).newInstance();
			String connectionURL = url + databasebName + "?useUnicode=yes&characterEncoding=UTF-8";
//			System.out.println(connectionURL);
			try {
				result = DriverManager.getConnection(connectionURL,user,pass);
			} catch (SQLException e) {
				System.out.println("Can't connect!");
				System.exit(0);
				e.printStackTrace();
			}
		return result;
	}

	public static void main(String[] args) throws Exception {
//		Connection conn = new GetConnection().getConnection("control");
//		if (conn != null) {
//			System.out.println("Successfully");
//		}
		 String s = "191924|hsjabro jsdfkj asdkj@aks";
		 System.out.println(s.replace("|", ",").replace(" ", ","));
	}
}
