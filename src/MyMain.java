
import java.util.Timer;
import java.util.TimerTask;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import load_staging_to_warehouse.StagingtoWarehouse;

public class MyMain {
	static DownloadScp dowload = new DownloadScp();
	static StagingtoWarehouse loadWH = new StagingtoWarehouse();

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
		Timer timer = new Timer();
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				try {
					loadWH.LoadStagingtoWarehouse(Integer.parseInt(args[0]));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		};

		timer.scheduleAtFixedRate(task, 0, 60000);

	}

}
