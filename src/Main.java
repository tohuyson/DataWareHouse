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
