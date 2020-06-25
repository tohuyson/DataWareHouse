import com.chilkatsoft.CkScp;
import com.chilkatsoft.CkSsh;




public class b {
	

	  static {
	    try {
	        System.loadLibrary("chilkat");
	    } catch (UnsatisfiedLinkError e) {
	      System.err.println("Native code library failed to load.\n" + e);
	      System.exit(1);
	    }
	  }

	  public static void main(String argv[])
	  {
	    // This example requires the Chilkat API to have been previously unlocked.
	    // See Global Unlock Sample for sample code.

	    CkSsh ssh = new CkSsh ();

	    // Connect to an SSH server:
	    String hostname;
	    int port;

	    // Hostname may be an IP address or hostname:
	    hostname = "drive.ecepvn.org";
	    port = 2227;

	    boolean success = ssh.Connect(hostname,port);
	    if (success != true) {
	        System.out.println(ssh.lastErrorText());
	        return;
	        } 

	    // Wait a max of 5 seconds when reading responses..
	    ssh.put_IdleTimeoutMs(5000);

	    // Authenticate using login/password:
	    success = ssh.AuthenticatePw("guest_access","123456");
	    if (success != true) {
	        System.out.println(ssh.lastErrorText());
	        return;
	        }

	    // Once the SSH object is connected and authenticated, we use it
	    // as the underlying transport in our SCP object.
	    CkScp scp = new CkScp();

	    success = scp.UseSsh(ssh);
	    if (success != true) {
	        System.out.println(scp.lastErrorText());
	        return;
	        }

	    // This uploads a file to the "uploads/text" directory relative to the HOME
	    // directory of the SSH user account.  For example, if the HOME directory is /home/chilkat,
	    // then this uploads to /home/chilkat/uploads/text/test.txt
	    // Note: The remote target directory must already exist on the SSH server.
	    scp.put_SyncMustMatch("sinhvien*.*");
	    String remotePath = "/volum1/ECEP/song.nguyen/DW_2020/data";
	    String localPath = "D://CNTT/DataWarehouse/";
	    success = scp.SyncTreeDownload(remotePath, localPath, 2, false);
	    if (success != true) {
	        System.out.println(scp.lastErrorText());
	        return;
	        }

//	    // This upload fully specifies the absolute remote path.
//	    remotePath = "/ECEP/song.nguyen/DW_2020/data/sinhvien_sang_nhom6.xlsx";
//	    localPath = "D://CNTT";
//	    success = scp.UploadFile(remotePath, localPath);
//	    if (success != true) {
//			System.out.println(scp.lastErrorText());
//			return;
//		}
	    // Disconnect
	    ssh.Disconnect();
	  }
	}

