
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
//			loadStaging.staging("my_logs.status_download = 'OK Download' AND my_logs.id_config=" + args[0]);
			loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
