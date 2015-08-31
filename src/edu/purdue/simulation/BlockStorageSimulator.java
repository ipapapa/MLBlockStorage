package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

import edu.purdue.simulation.blockstorage.*;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEvent;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;

public class BlockStorageSimulator {

	public static HashSet<VolumeRequest> GenerateRequests() {

		return null;

		// randomly generate requests
	}

	public static void retrieveExperiment(BigDecimal experimentID)
			throws SQLException {

		// Experiment experiment = Experiment.resumeExperiment(experimentID);

	}

	public static void main(String[] args) {

		StochasticEvent.saveStochasticEvents = false;

		ResourceMonitor.enableBackendPerformanceMeter = false;

		ResourceMonitor.enableVolumePerformanceMeter = true;

		ResourceMonitor.clockGap = 5;

		ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = false;

		Scheduler.maxClock = 10000;// 110000;

		Scheduler.modClockBy = 1440;

		Scheduler.schedulePausePoissonMean = 5; // not used

		Scheduler.devideVolumeDeleteProbability = 3; // not used

		Scheduler.minRequests = 5000;

		// Scheduler.considerIOPS = false;

		Scheduler.isTraining = false;

		Scheduler.trainingExperimentID = 689;

		StochasticEventGenerator.clockGap = 4;

		StochasticEventGenerator.applyToAllBackends = true;

		Experiment.saveResultPath = "D:\\Dropbox\\Research\\experiment\\";

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

					Database.getConnection().close();
				} catch (Exception ex) {

					System.out.println("***ERROR***\n   " + ex.getMessage()
							+ "\n***ERROR***");

					ex.printStackTrace();

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
