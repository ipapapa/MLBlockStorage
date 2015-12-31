package edu.purdue.simulation;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.mysql.jdbc.Statement;

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

	public static Map<Integer, Object[][]> feedbackAccuracy = new HashMap<Integer, Object[][]>();

	public static Map<AssessmentPolicy, String> assessmentPolicyRules = new HashMap<AssessmentPolicy, String>();

	public static void main(String[] args) throws IOException,
			InterruptedException {

		Scheduler.isTraining = false;

		Scheduler.trainingExperimentID = 2;
		Scheduler.assessmentPolicy = AssessmentPolicy.StrictQoS;
		Scheduler.machineLearningAlgorithm = MachineLearningAlgorithm.BayesianNetwork;

		Scheduler.feedBackLearning = false;
		Scheduler.feedbackLearningInterval = 300;
		Scheduler.updateLearning_MinNumberOfRecords = Scheduler.feedbackLearningInterval * 2;
		Scheduler.updateLearning_MaxNumberOfRecords = 1500;

		Scheduler.modClockBy = 300; // every 300 minutes
		StochasticEventGenerator.clockGapProbability = 250;
		ResourceMonitor.clockGapProbability = 0.5;

		int workloadID = 161; // training workload
		Workload.devideDeleteFactorBy = 2.5;
		Scheduler.maxRequest = 10000;// 110000;
		Scheduler.startTestDatasetFromRecordRank = 10000;

		Scheduler.violationGroups = "V1: 0 - V2: [1, 2] - V3: [3, 4] - V5: > 4";

		ResourceMonitor.enableVolumePerformanceMeter = true;
		Experiment.saveResultPath = "D:\\Dropbox\\Research\\MLScheduler\\experiment\\";
		// Condition must hold -> Scheduler.maxClock > Scheduler.minRequests
		Scheduler.minRequests = 0; // 0 means no minimum requests

		/*
		 * don't change it, let it be false, if there is no volume, then how to
		 * measure a backend available IOPS (black box approach). The learning
		 * algorithm should create a good enough classifier without having this
		 * data
		 */
		ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = false;

		Scheduler.schedulePausePoissonMean = 5; // not used

		StochasticEventGenerator.applyToAllBackends = true; // not used

		/*
		 * SQL procedures to create report for this section are lost still you
		 * can have this switch to record bacjends behaviour such as stochastic
		 * event, but it will slow down the process unless batch query be
		 * applied
		 */
		ResourceMonitor.enableBackendPerformanceMeter = false; // not used

		StochasticEvent.saveStochasticEvents = false; // not used

		if (Scheduler.isTraining == true)

			Scheduler.feedBackLearning = false;

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

					BlockStorageSimulator.recordReport(experiment);

					System.out.println(experiment.toString() + "\n");

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
									.size()) + "assessmentPolicy: "
							+ Scheduler.assessmentPolicy);

					double count_accuracyRecords = 0;
					double total_accuracy = 0;

					if (Scheduler.feedBackLearning) {

						for (Integer key : BlockStorageSimulator.feedbackAccuracy
								.keySet()) {

							System.out.print("clock: " + key + " - ");

							for (Object[] accuracyRecord : BlockStorageSimulator.feedbackAccuracy
									.get(key)) {
								count_accuracyRecords++;

								if (accuracyRecord[1] == null) {
									accuracyRecord[1] = (double) 0.0;
								}

								total_accuracy = total_accuracy
										+ (double) accuracyRecord[1];

								System.out.print(Math
										.round((double) accuracyRecord[1])
										+ "%, ");
							}

							System.out.println();
						}

						System.out
								.println("\nFeedback learning average acuracy :"
										+ (total_accuracy / count_accuracyRecords)
										+ ", #created models: "
										+ BlockStorageSimulator.feedbackAccuracy
												.size()
										+ ", # accuracy records: "
										+ count_accuracyRecords);
					}

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

					//

					Thread.sleep(4000);

					copyFile(new File(
							"D:\\Dropbox\\Research\\experiment\\output.txt"),
							new File("D:\\Dropbox\\Research\\experiment\\"
									+ experiment.getID().intValue() + ".txt"));

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

	public static void recordReport(Experiment ex) throws SQLException {

		Connection connection = Database.getConnection();

		String query = "INSERT INTO blockstoragesimulator.experiment_report ("
				+ "Experiment_ID, Is_Feedback_Learning, Is_Training, all_backend_QoS, rejection_rate,"
				+ "requests_count, scheduled_vol_count, rejected_vol_count, deleted_vol_count,"
				+ "avg_requested_capacity, avg_IOPS_requested, avg_available_IOPS, avg_deletion_time,"
				+ "SLA_vio_count, vol_performance_meter_count, max_clock, backend_count, predictors_rules,"
				+ "vio_groups, ML_algorithm,clock_unit, "
				+ "Assessment_Policy, training_experiment_ID, Resource_Monitor_clockGap_probability,"
				+ "Scheduler_modClockBy, Scheduler_Feedback_Learning_Interval, "
				+ "Stochastic_Event_Generator_clockGap_probability,"
				+ "update_Learning_Min_Number_Of_Records, update_Learning_Max_Number_Of_Records) VALUES ( "
				+ "?," // Experiment_ID
				+ "?," // Is_Feedback_Learning
				+ "?," // Is_Training
				+ "?," // all_backend_QoS
				+ "?," // rejection_rate
				+ "?," // requests_count
				+ "?," // scheduled_vol_count
				+ "?," // rejected_vol_count
				+ "?," // deleted_vol_count
				+ "?," // avg_requested_capacity
				+ "?," // avg_IOPS_requested
				+ "?," // avg_available_IOPS
				+ "?," // avg_deletion_time
				+ "?," // SLA_vio_count
				+ "?," // vol_performance_meter_count
				+ "?," // max_clock
				+ "?," // backend_count
				+ "?," // predictors_rules
				+ "?," // vio_groups
				+ "?," // ML_algorithm
				+ "?," // clock_unit
				+ "?," // Assessment_Policy
				+ "?," // training_experiment_ID
				+ "?," // Resource_Monitor_clockGap_probability
				+ "?," // Scheduler_modClockBy
				+ "?," // Scheduler_Feedback_Learning_Interval
				+ "?," // Stochastic_Event_Generator_clockGap_probability
				+ "?," // update_Learning_Min_Number_Of_Records
				+ "? " // update_Learning_Max_Number_Of_Records
				+ ");";

		PreparedStatement statement = connection.prepareStatement(query);

		PreparedStatement report = connection
				.prepareCall("{call experiment_report(?)}");

		report.setBigDecimal(1, ex.getID());
		report.execute();
		ResultSet rs = report.getResultSet();
		rs.next();

		statement.setBigDecimal(1, ex.getID()); // Experiment_ID
		statement.setBoolean(2, Scheduler.feedBackLearning); // Is_Feedback_Learning
		statement.setBoolean(3, Scheduler.isTraining);// Is_Training

		statement.setFloat(4, rs.getFloat(1)); // all_backend_QoS
		statement.setFloat(5, rs.getFloat(2)); // rejection_rate
		statement.setInt(6, rs.getInt(3)); // requests_count
		statement.setInt(7, rs.getInt(4)); // scheduled_vol_count
		statement.setInt(8, rs.getInt(5)); // rejected_vol_count
		statement.setInt(9, rs.getInt(6)); // deleted_vol_count
		statement.setFloat(10, rs.getFloat(7)); // avg_requested_capacity
		statement.setFloat(11, rs.getFloat(8)); // avg_IOPS_requested
		statement.setFloat(12, rs.getFloat(9)); // avg_available_IOPS
		statement.setFloat(13, rs.getFloat(10)); // avg_deletion_time
		statement.setInt(14, rs.getInt(11)); // SLA_vio_count
		statement.setInt(15, rs.getInt(12)); // vol_performance_meter_count
		statement.setInt(16, rs.getInt(13)); // max_clock
		statement.setInt(17, rs.getInt(14)); // backend_count

		String predictionRule = BlockStorageSimulator.assessmentPolicyRules
				.get(Scheduler.assessmentPolicy);
		if (predictionRule == null)
			predictionRule = "";

		statement.setString(18, predictionRule); // predictors_rules

		statement.setString(19, Scheduler.violationGroups); // vio_groups
		statement.setString(20, Scheduler.machineLearningAlgorithm.toString()); // ML_algorithm
		statement.setString(21, ""); // clock_unit

		statement.setString(22, Scheduler.assessmentPolicy.toString()); // Assessment_Policy
		statement.setInt(23, Scheduler.trainingExperimentID); // training_experiment_ID
		statement.setDouble(24, ResourceMonitor.clockGapProbability); // Resource_Monitor_clockGap_probability
		statement.setInt(25, Scheduler.modClockBy); // Scheduler_modClockBy
		statement.setInt(26, Scheduler.feedbackLearningInterval);// Scheduler_Feedback_Learning_Interval
		// Stochastic_Event_Generator_clockGap_probabilityDouble
		statement.setDouble(27, StochasticEventGenerator.clockGapProbability);
		// update_Learning_Min_Number_Of_Records
		statement.setInt(28, Scheduler.updateLearning_MinNumberOfRecords);
		// update_Learning_Max_Number_Of_Records
		statement.setInt(29, Scheduler.updateLearning_MaxNumberOfRecords);

		statement.execute();
	}

	public static void copyFile(File sourceFile, File destFile)
			throws IOException {
		if (!destFile.exists()) {
			destFile.createNewFile();
		}

		FileChannel source = null;
		FileChannel destination = null;

		try {
			source = new FileInputStream(sourceFile).getChannel();
			destination = new FileOutputStream(destFile).getChannel();
			destination.transferFrom(source, 0, source.size());
		} finally {
			if (source != null) {
				source.close();
			}
			if (destination != null) {
				destination.close();
			}
		}
	}
}
