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

		ResourceMonitor.enableBackendPerformanceMeter = true;

		ResourceMonitor.enableVolumePerformanceMeter = true;

		ResourceMonitor.clockGap = 1;

		Scheduler.maxClock = 500;

		Scheduler.schedulePausePoissonMean = 5;

		Scheduler.devideVolumeDeleteProbability = 3;

		Scheduler.considerIOPS = false;

		StochasticEventGenerator.clockGap = 4;

		try {

			// Random r = new Random();
			//
			// for (int i = 0; i < 100; i++) {
			// System.out.println(r.nextDouble());
			// }

			// retrieveExperiment(BigDecimal.valueOf(331));

			// for (int i = 0; i < 600; i++) {
			// System.out.println(i + "\t" + getPoissonRandom(1000));
			// }

			// args = null;

			if (args != null) {

				System.out.println("start");

				// Workload workload = new Workload(1 // generate method
				// , "workload with time gap");
				//
				// workload.GenerateWorkload2();

				Workload workload = new Workload(BigDecimal.valueOf(37));

				workload.RetrieveVolumeRequests();

				// Experiment experiment = new Experiment(
				// "MostAvailableCapacity Run 1", "MostAvailableCapacity");

				Experiment experiment = new Experiment(workload, "", "");

				//
				experiment.save();
				//
				// experiment.GenerateBackEnd();
				//
				// Scheduler scheduler = new
				// edu.purdue.simulation.blockstorage.StatisticalGroupping(
				// experiment, workload);

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
					for (int i = 0; i < Experiment.backEndList.size(); i++) {
						scheduler.getExperiment();
						System.out.println("Backend ID = "
								+ Experiment.backEndList.get(i).getID());
					}

					System.out.println("sum: " + Scheduler.sum
							+ " randGeneratedNumbers "
							+ Scheduler.randGeneratedNumbers + " mean: "
							+ (Scheduler.sum / Scheduler.randGeneratedNumbers));

					System.out.println("Clock: " + Experiment.clock);

					System.out
							.println("**Remmeber MAX clock number is: " + 90213);
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
