package load_staging_to_warehouse;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.StringTokenizer;

import com.mysql.jdbc.DatabaseMetaData;
import com.mysql.jdbc.PreparedStatement;

import connect.GetConnection;
import load_local_to_staging.LoadFromLocalToStaging;

public class LoadDataDim {
	public static void loadDataDim() throws Exception {	
		Connection connWareHouse = new GetConnection().getConnection("warehouse");LoadFromLocalToStaging staging = new LoadFromLocalToStaging();
		File file_date_dim = new File("D:\\Data\\date\\date_dim_without_quarte_st.csv");
		String list = staging.readStudentsFromFile(file_date_dim, 18);
		String sql = "INSERT INTO datadim VALUES " + list;
		PreparedStatement pre = (PreparedStatement) connWareHouse.prepareStatement(sql);
		pre.execute();
	}

	public static void main(String[] args) throws Exception {
		new LoadDataDim().loadDataDim();
	}
}
