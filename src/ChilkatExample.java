

import com.chilkatsoft.CkGlobal;
import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;

public class ChilkatExample {
	static {
		try {
			System.loadLibrary("lib\\chilkat"); //copy file chilkat.dll vao thu muc project
		} catch (UnsatisfiedLinkError e) {
			System.err.println("Native code library failed to load.\n" + e);
			System.exit(1);
		}
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
//		scp.put_SyncMustMatch("sinhvien*.*");//down tat ca cac file bat dau bang sinhvien
		scp.put_SyncMustMatch("*.*");// download tat ca cac file 
		String remotePath = "/volume1/ECEP/song.nguyen/DW_2020/data";
		String localPath = "D://CNTT/DataWarehouse"; //vi tri file dich
		success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
		if (success != true) {
			System.out.println(scp.lastErrorText());
			return;
		}
//		System.out.println("success download file");
		ssh.Disconnect();
	}
}
