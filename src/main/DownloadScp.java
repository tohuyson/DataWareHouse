package main;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import connect.GetConnection;

public class DownloadScp {
	static {
		try {
			System.loadLibrary("chilkat"); // load thu vien chilkat
		} 
		// kiem tra ket noi chilkat là có thấy file chilkat.dll hay không ? 
		// Nếu không tìm thấy .dll thì báo lỗi
		catch (UnsatisfiedLinkError e) { 
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}

	}

	public static void downloadFile(String id_config) throws Exception {
		// connect database control
		Connection conn = new GetConnection().getConnection("control");
		try {
			Statement sta = conn.createStatement();
			// tạo statement, cau lenh sql lay thong tin tu bang config
			// (host_name,port,user_name,password,remote_path,local,fame_file_type)
			String sql = " select * from my_configs where id= " + id_config;
			// tao resultset de lay tung dong record
			ResultSet rs = sta.executeQuery(sql);
			while (rs.next()) {
				String hostname = rs.getString("host_name");
				int port = rs.getInt("port");
				String username = rs.getString("user_name");
				String pwd = rs.getString("password");
				String remotePath = rs.getString("remote_path");
				String localPath = rs.getString("local_path");
				String name_file_type = rs.getString("name_file_type");
				String id = String.valueOf(rs.getInt(1));

				// tao ckssh, ckGlobal
				CkSsh ssh = new CkSsh();
				CkGlobal ck = new CkGlobal();
				ck.UnlockBundle("Waiting...............");
				boolean success = ssh.Connect(hostname, port);
				if (success != true) {
					System.out.println(ssh.lastErrorText());
//					boolean s = new SendMail().sendMail("phantrancongthanh240499@gmail.com", "Thông báo", "Kết nối thất bại");
					SendMail.sendMail("phantrancongthanh240499@gmail.com", "Thông báo", "Kết nối thất bại");
					return;
				} 
				// chờ tối đa 5s để đọc phản hồi
				ssh.put_IdleTimeoutMs(5000);
				
				// Xác thực bằng đăng nhập / mật khẩu: 
				success = ssh.AuthenticatePw(username, pwd);
				if (success != true) {
					System.out.println(ssh.lastErrorText());
					SendMail.sendMail("phantrancongthanh240499@gmail.com", "Thông báo", "Sai username or password");
					return;
				}
				
				// Sau khi đối tượng SSH được kết nối và xác thực, sử dụngtrong đối tượng SCP . 
				CkScp scp = new CkScp();

				success = scp.UseSsh(ssh);
				if (success != true) {
					System.out.println(scp.lastErrorText());
					SendMail.sendMail("phantrancongthanh240499@gmail.com", "Thông báo", "Kết nối thất bại");
					return;
				}
				// lay ra cac file = name_file_type
				scp.put_SyncMustMatch(name_file_type);
				//thuc hien download ve localPath
				// Tải xuống các chế độ đồng bộ hóa: 
			    // mode = 0: Tải xuống tất cả các tệp 
			    // mode = 1: Tải xuống tất cả các tệp không tồn tại trên hệ thống tệp cục bộ. 
			    // mode = 2: Tải xuống các tệp mới hơn hoặc không tồn tại. 
			    // mode = 3: Chỉ tải xuống các tệp mới hơn.  
			    // Nếu một tệp chưa tồn tại trên hệ thống tệp cục bộ, nó sẽ không được tải xuống từ máy chủ. 
			    // mode = 5: Chỉ tải xuống các tệp bị thiếu hoặc các tệp có kích thước khác nhau. 
			    // mode = 6: Tương tự như mode 5, nhưng cũng tải xuống các tệp mới hơn. 
				
				// sử dụng mode = 2 để download các file mới
				success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
				if (success != true) {
					
					System.out.println(scp.lastErrorText());
					return;
				} else {
					try {
						// load file vao database control table
						logDownloadFile(new File(localPath), id);
						SendMail.sendMail("phantrancongthanh240499@gmail.com", "Thông báo", "Ghi log Status download : OK Download");
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				System.out.println("Done !!!");
				SendMail.sendMail("phantrancongthanh240499@gmail.com", "Thông báo", "Done !!! ");
				// ngắt kết nối
				ssh.Disconnect();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void logDownloadFile(File folder, String id) throws Exception {
		// connect database control
		Connection conn = new GetConnection().getConnection("control");

		String sql = "insert into my_logs"
				+ "(id_config,status_download,date_time_download,local_path,name_file_local,extension,status_stagging,date_time_staging,load_row_stagging,status_warehouse,date_time_warehouse,load_row_warehouse)"
				+ "values" + "(?,?,?,?,?,?,?,?,?,?,?,?)";
		PreparedStatement sta = conn.prepareStatement(sql);
		for (File f : folder.listFiles()) {
			sta.setInt(1, Integer.parseInt(id));
			sta.setString(2, "OK Download");
			sta.setDate(3, new Date(System.currentTimeMillis()));
			sta.setString(4, f.getParent() + "\\");
			sta.setString(5, f.getName().substring(0, f.getName().indexOf(".")));
			sta.setString(6, f.getName().substring(f.getName().lastIndexOf(".")));
			sta.setString(7, "Error Stagging");
			sta.setDate(8, new Date(System.currentTimeMillis()));
			sta.setString(9, "-1");
			sta.setString(10, "Error Warehouse");
			sta.setDate(11, new Date(System.currentTimeMillis()));
			sta.setString(12, "-1");
			sta.execute();

		}
		sta.close();
		conn.close();
	}

	public static void main(String argv[]) throws Exception {
//		DownloadScp.downloadFile("1");  
//		DownloadScp.downloadFile("3");
//		DownloadScp.downloadFile("4");
//		System.out.println(SendMail.sendMail("phantrancongthanh240499@gmail.com","hihihi","lay lai mat khau"));

	}
}
