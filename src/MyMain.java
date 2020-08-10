
import java.util.Timer;
import java.util.TimerTask;

import load_staging_to_warehouse.StagingtoWarehouse;
import main.DownloadScp;

public class MyMain {

	static DownloadScp dowload = new DownloadScp();
	static StagingtoWarehouse loadWH = new StagingtoWarehouse();

	public static void main(String[] args) {
		try {
			System.out.println(args[0]);
			dowload.downloadFile(args[0]);
			loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
		} catch (Exception e) {
			e.printStackTrace();
		}



	
	}
}
