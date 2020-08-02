package main;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

import load_local_to_staging.LoadFromLocalToStaging;

public class Main extends TimerTask {

	String id;
	DownloadScp dow = new DownloadScp();
	LoadFromLocalToStaging loadStaging = new LoadFromLocalToStaging();

	public Main(String args) {
		this.id = args;
	}

	@Override
	public void run() {
		System.out.println("Run my Task " + new Date());
		try {

			dow.downloadFile(id);
			loadStaging.staging("my_logs.status_download = 'OK Download' AND my_logs.id_config=" + id);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Main myTask = new Main(args[0]);
		Timer timer = new Timer();
		System.out.println("Currnet time: " + new Date());
		timer.schedule(myTask, 40, 60000 * 2);

	}
}
