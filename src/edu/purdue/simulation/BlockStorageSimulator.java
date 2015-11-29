package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.*;

import edu.purdue.simulation.blockstorage.*;
import edu.purdue.simulation.blockstorage.backend.Backend;
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

		/*
		 * SQL procedures to create report for this section are lost still you
		 * can have this switch to record bacjends behaviour such as stochastic
		 * event, but it will slow down the process unless batch query be
		 * applied
		 */
		ResourceMonitor.enableBackendPerformanceMeter = false;

		ResourceMonitor.enableVolumePerformanceMeter = true;

		ResourceMonitor.clockGap = 5;

		ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = false;

		// Condition must hold -> Scheduler.maxClock > Scheduler.minRequests
		Scheduler.minRequests = 0; // 0 means no minimum requests

		Scheduler.modClockBy = 1440;

		Scheduler.schedulePausePoissonMean = 5; // not used

		Scheduler.devideVolumeDeleteProbability = 3; // not used

		Scheduler.machineLearningAlgorithm = MachineLearningAlgorithm.J48;

		// Scheduler.considerIOPS = false;

		Workload.devideDeleteFactorBy = 2.5;

		Scheduler.maxRequest = 5000;// 110000;

		Scheduler.isTraining = false;

		Scheduler.assessmentPolicy = AssessmentPolicy.EfficiencyFirst;

		Scheduler.trainingExperimentID = 824;

		int workloadID = 161; // 161

		StochasticEventGenerator.clockGap = 4;

		StochasticEventGenerator.applyToAllBackends = true;

		Experiment.saveResultPath = "D:\\Dropbox\\Research\\MLScheduler\\experiment\\";

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

					double sumClassifierEvaluation = 0;

					for (int i = 0; i < Experiment.backendList.size(); i++) {
						scheduler.getExperiment();

						Backend backend = Experiment.backendList.get(i);

						String classifierEvalString = " Classifier Accuracy: None";

						if (backend.classifierEvaluation != null) {

							sumClassifierEvaluation += backend.classifierEvaluation
									.pctCorrect();

							classifierEvalString = " Classifier Accuracy: "
									+ backend.classifierEvaluation.pctCorrect();
						}

						System.out.println("Backend ID = " + backend.getID()
								+ classifierEvalString);
					}

					System.out.println("simulation done, experiment ID ="
							+ experiment.getID()
							+ "\nAverage Backends Classifiers Accuracy: "
							+ (sumClassifierEvaluation / Experiment.backendList
									.size()));

					if (Scheduler.isTraining == false)

						System.out
								.println("training dataset from experiment exp#"
										+ Scheduler.trainingExperimentID);
					/*
					 * System.out.println("sum: " + Scheduler.sum +
					 * " randGeneratedNumbers " + Scheduler.randGeneratedNumbers
					 * + " mean: " + (Scheduler.sum /
					 * Scheduler.randGeneratedNumbers));
					 */

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
