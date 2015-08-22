package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

import edu.purdue.simulation.blockstorage.*;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;

public class BlockStorageSimulator {

	public static HashSet<VolumeRequest> GenerateRequests() {

		return null;

		// randomly generate requests
	}

	public static void retrieveExperiment(BigDecimal experimentID)
			throws SQLException {

		Experiment experiment = Experiment.resumeExperiment(experimentID);

	}

	public static void main(String[] args) {

		ResourceMonitor.enableBackendPerformanceMeter = false;

		ResourceMonitor.enableVolumePerformanceMeter = true;

		ResourceMonitor.clockGap = 5;

		ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = false;

		Scheduler.maxClock = 110000;// 110000;

		Scheduler.schedulePausePoissonMean = 5; // not used

		Scheduler.devideVolumeDeleteProbability = 3; // not used

		Scheduler.considerIOPS = false;

		Scheduler.isTraining = false;

		StochasticEventGenerator.clockGap = 4;

		int workloadID = 62;

		try {

			if (args != null) {

				System.out.println("start");

				Workload workload = new Workload(BigDecimal.valueOf(workloadID));

				workload.RetrieveVolumeRequests();

				Experiment experiment = new Experiment(workload, "", "");

				experiment.save();

				Scheduler scheduler = new edu.purdue.simulation.blockstorage.MaxCapacityFirstScheduler(
						experiment, workload);

				try {
					scheduler.run();
				} catch (Exception ex) {
					System.out.println("ERROR" + ex.getMessage());
				} finally {
					System.out.println("simulation done, experiment ID ="
							+ experiment.getID());

					scheduler.getExperiment();
					for (int i = 0; i < Experiment.backendList.size(); i++) {
						scheduler.getExperiment();
						System.out.println("Backend ID = "
								+ Experiment.backendList.get(i).getID());
					}

					System.out.println("sum: " + Scheduler.sum
							+ " randGeneratedNumbers "
							+ Scheduler.randGeneratedNumbers + " mean: "
							+ (Scheduler.sum / Scheduler.randGeneratedNumbers));

					System.out.println("Clock: " + Experiment.clock);

					// System.out
					// .println("**Remmeber MAX clock number is: " + 90213);
				}
			}

			// System.out.println("\n\nend - Experiment ID: "+
			// experiment.ID.toString() + " Workload ;ID: +
			// workload.ID.toString());

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

	}
}
