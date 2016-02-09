import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

public class AutomateExperiment_Run implements Runnable {

	public static int activeThreadCount = 0;

	private String cmd;

	private String threadName;

	public AutomateExperiment_Run(String cmd, String threadName) {
		this.cmd = cmd;

		this.threadName = threadName;
	}

	public void run() {

		activeThreadCount++;

		Process p;
		
		try {
			p = Runtime.getRuntime().exec(this.cmd);

			p.waitFor();
			
			if (p.waitFor(25, TimeUnit.MINUTES)) {
				System.out.println("Done ! " + this.threadName + "-" + DateTime.now());
			} else {
				System.out.println("[TIME OUT 25 min] ! " + this.threadName + "-" + DateTime.now());
			}
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

		activeThreadCount--;
	}

}
