
import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

import connect.GetConnection;


public class DownloadScp {
	static {
		try {
			System.loadLibrary("chilkat"); // copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
	}
 
	public static void logDownloadFile(File folder) throws Exception {
		// chua kiem tra local_path da ton tai hay chua
		Connection conn = new GetConnection().getConnection("control");
		String sql =  "insert into my_logs" + "(id_config,status_download,date_time_download,local_path,name_file_local,extension,status_stagging,date_time_staging,load_row_stagging,record_end,status_warehouse,date_time_warehouse,load_row_warehouse)" + "values"
				+ "(?,?,?,?,?,?,?,?,?,?,?,?,?)" ; 
		PreparedStatement sta = conn.prepareStatement(sql);
		for (File f : folder.listFiles()) {
				sta.setString(1, "1");
				sta.setString(2, "OK Download");
				sta.setDate(3, new Date(System.currentTimeMillis()));
				sta.setString(4, f.getParent() + "\\");
				sta.setString(5, f.getName().substring(0,f.getName().indexOf(".")));
				sta.setString(6, f.getName().substring(f.getName().lastIndexOf(".") ));
				sta.setString(7, "Error Stagging");
				sta.setDate(8, new Date(System.currentTimeMillis()));
				sta.setString(9, "-1");
				sta.setString(10, "-1");
				sta.setString(11, "Error Warehouse");
				sta.setDate(12, new Date(System.currentTimeMillis()));
				
				sta.setString(13, "-1");
//				sta.setString(13, "-1");
				sta.execute();
		}
		sta.close(); 
		conn.close();
	}

	public static void main(String argv[]) {
		CkSsh ssh = new CkSsh();
		CkGlobal ck = new CkGlobal();
		ck.UnlockBundle("Hello Phan Tran Cong Thanh");
		String hostname = "drive.ecepvn.org";
		int port = 2227;
		boolean success = ssh.Connect(hostname, port);
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}

		ssh.put_IdleTimeoutMs(5000);
		success = ssh.AuthenticatePw("guest_access", "123456");
		if (success != true) {
			System.out.println(ssh.lastErrorText());
			return;
		}
		CkScp scp = new CkScp();

		success = scp.UseSsh(ssh);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
		scp.put_SyncMustMatch("sinhvien*.*");//down tat ca cac file bat dau bang sinhvien sao ko chay dong` nay? phan down nay dang test
//		scp.put_SyncMustMatch("*.*");// download tat ca cac file
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		String localPath = "D:\\Data\\"; // vi tri file dich
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		} else {
			try {
					logDownloadFile(new File(localPath));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

//		System.out.println("success download file");
		ssh.Disconnect();

	}
}
