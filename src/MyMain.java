
import java.util.Timer;
import java.util.TimerTask;

import load_staging_to_warehouse.StagingtoWarehouse;

public class MyMain {
//	static DownloadScp dowload = new DownloadScp();

//	public static void main(String[] args) {
//		try {
//			System.out.println(args[0]);
//			dowload.downloadFile(Integer.parseInt(args[0]));
////			loadStaging.staging("my_logs.status_download = 'OK Download' AND my_logs.id_config=" + args[0]);
//			loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}

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
