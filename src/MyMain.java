
import java.util.Timer;
import java.util.TimerTask;

import load_staging_to_warehouse.StagingtoWarehouse;

public class MyMain {
	// static DownloadScp dowload = new DownloadScp();
	// static LoadFromLocalToStaging loadStaging= new LoadFromLocalToStaging();
	//
	// public static void main(String[] args) {
	// try {
	// System.out.println(args[0]);
	//// dowload.downloadFile(Integer.parseInt(args[0]));
	// loadStaging.staging("OK Download", Integer.parseInt(args[0]),7);
	//// loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
	// } catch (Exception e) {
	// e.printStackTrace();
	// }
	// }

	public static void main(String[] args) {
		StagingtoWarehouse loadWH = new StagingtoWarehouse();
		Timer timer = new Timer();
		TimerTask task = new TimerTask() {

			int count = 1;

			@Override
			public void run() {
				try {

					System.out.println(" thực hiện lần thứ: " + count);
					loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
					count++;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};

		timer.scheduleAtFixedRate(task, 0, 60000);

	}

}
