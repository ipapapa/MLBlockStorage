package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

import edu.purdue.simulation.blockstorage.*;

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

				Experiment experiment = new Experiment(workload,
						"StatisticalGroupping Run 1", "StatisticalGroupping");

				//
				experiment.Save();
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

					for (int i = 0; i < scheduler.getExperiment().backEndList
							.size(); i++) {
						System.out.println("Backend ID = "
								+ scheduler.getExperiment().backEndList.get(i)
										.getID());
					}

					System.out.println("sum: " + Scheduler.sum
							+ " randGeneratedNumbers "
							+ Scheduler.randGeneratedNumbers + " mean: "
							+ (Scheduler.sum / Scheduler.randGeneratedNumbers));
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

	private static int getPoissonRandom(double mean) {
		Random r = new Random();

		double L = Math.exp(-mean);
		int k = 0;
		double p = 1.0;
		do {
			p = p * r.nextDouble();
			k++;
		} while (p > L);
		return k - 1;
	}
}
