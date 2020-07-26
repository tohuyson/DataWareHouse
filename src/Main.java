<<<<<<< HEAD
import load_local_to_staging.LoadFromLocalToStaging;
import load_staging_to_warehouse.StagingtoWarehouse;

public class Main {
	static DownloadScp dowload = new DownloadScp();
	static LoadFromLocalToStaging loadStaging = new LoadFromLocalToStaging();
	static StagingtoWarehouse loadWH = new StagingtoWarehouse();

	public static void main(String[] args) {
		try {
			System.out.println(args[0]);
//					dowload.downloadFile(Integer.parseInt(args[0]));
			loadStaging.staging("my_logs.status_download = 'OK Download' AND my_logs.id_config=" + args[0]);
			loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
=======
import load_local_to_staging.LoadFromLocalToStaging;
import load_staging_to_warehouse.StagingtoWarehouse;

public class Main extends Thread{
	DownloadScp dowload;
	LoadFromLocalToStaging loadStaging;
	StagingtoWarehouse loadWH;
	public Main() {
		
	}
	@Override
	public void run() {
		super.run();
		
	}
}
>>>>>>> 23b0ddb3d9844107510aea97a4e801c5f3f74e6d
