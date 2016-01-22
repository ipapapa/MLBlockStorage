package edu.purdue.simulation;

import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTime;

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

	public static String logPath;

	private static PrintWriter logWrite;

	private static Boolean hasCommandArgs = false;

	public static void main(String[] args) {

		Date startTime = new Date();

		Scheduler.isTraining = true;

		Scheduler.trainingExperimentID = 144;
		Scheduler.assessmentPolicy = AssessmentPolicy.MaxEfficiency;
		Scheduler.machineLearningAlgorithm = MachineLearningAlgorithm.BayesianNetwork;

		Scheduler.feedBackLearning = true;
		Scheduler.feedbackLearningInterval = 180; /**/
		Scheduler.updateLearning_MinNumberOfRecords = 500; // 300
		Scheduler.updateLearning_MaxNumberOfRecords = 700;// 900;

		Scheduler.modClockBy = 300; // every 300 minutes
		StochasticEventGenerator.clockGapProbability = 140; /**/
		ResourceMonitor.clockGapProbability = 1.1; /**/

		if (// Scheduler.feedBackLearning == false ||
		Scheduler.isTraining)

			StochasticEventGenerator.clockGapProbability = -1; // no stochastic
																// event

		Experiment.workloadID = 161; // training workload
		Workload.devideDeleteFactorBy = 2.5;
		Scheduler.maxRequest = 10000;// 110000;
		Scheduler.startTestDatasetFromRecordRank = 10000;

		Scheduler.violationGroups = "V1: 0 - V2: [1, 2] - V3: [3, 4] - V5: > 4";

		ResourceMonitor.enableVolumePerformanceMeter = true;
		Experiment.saveResultPath = "D:\\Dropbox\\Research\\MLScheduler\\experiment\\";
		BlockStorageSimulator.logPath = "D:\\Dropbox\\Research\\MLScheduler\\experiment_console_output\\";

		// Condition must hold -> Scheduler.maxClock > Scheduler.minRequests

		/*
		 * don't change it, let it be false, if there is no volume, then how to
		 * measure a backend available IOPS (black box approach). The learning
		 * algorithm should create a good enough classifier without having this
		 * data
		 */
		ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = false;

		// dont change, will break the simulator
		Scheduler.schedulePausePoissonMean = 5;

		// dont change, will break the simulator
		StochasticEventGenerator.applyToAllBackends = true;

		/*
		 * SQL procedures to create report for this section are lost still you
		 * can have this switch to record bacjends behaviour such as stochastic
		 * event, but it will slow down the process unless batch query be
		 * applied
		 */
		// dont change, will break the simulator
		ResourceMonitor.enableBackendPerformanceMeter = false;

		/*
		 * it might be useful to save/keep track of stochastic events. for
		 * example, I can see how many of them are applied due to backend
		 * max/min IOPS constraints. However, saving stochastic events involves
		 * inserting a record into the backend table
		 * (backend.saveCurrentState()) which is not done by batch insert and
		 * will slow the simulation significantly. I dont think
		 * backend.saveCurrentState() is necessary specially I can save the
		 * previous state of a backend in the stochastic_event tables itself.
		 * For now lets keep ot False.
		 */
		// dont change, will break the simulator
		StochasticEvent.saveStochasticEvents = false;

		// dont change, will break the simulator
		Scheduler.minRequests = 0;

		Workload workload = null;
		Experiment experiment = null;
		Scheduler scheduler = null;

		try {
			applyCommandArguments(args);

			if (Scheduler.isTraining == true
					|| Scheduler.assessmentPolicy == AssessmentPolicy.MaxEfficiency)

				Scheduler.feedBackLearning = false;

			workload = new Workload(BigDecimal.valueOf(Experiment.workloadID));

			workload.RetrieveVolumeRequests();

			experiment = new Experiment(workload, "", "");

			experiment.save();

			BlockStorageSimulator.logWrite = new PrintWriter(
					BlockStorageSimulator.logPath
							+ experiment.getID().toString() + ".txt", "UTF-8");

			BlockStorageSimulator.log("start " + startTime.toString());

			scheduler = new edu.purdue.simulation.blockstorage.MaxCapacityFirstScheduler(
					experiment, workload);

			scheduler.run();

			Database.getConnection().close();
		} catch (Exception ex) {

			edu.purdue.simulation.BlockStorageSimulator.log(ex, "main");

		} finally {

			try {
				long simulation_duration = BlockStorageSimulator.getDateDiff(//
						startTime,//
						new Date(),//
						TimeUnit.SECONDS);

				BlockStorageSimulator.recordReport(experiment,
						simulation_duration);

				edu.purdue.simulation.BlockStorageSimulator.log(experiment
						.toString() + "\n");

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

					edu.purdue.simulation.BlockStorageSimulator
							.log("Backend ID = " + backend.getID()
									+ classifierEvalString);
				}

				edu.purdue.simulation.BlockStorageSimulator
						.log("simulation done, experiment ID ="
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
									.round((double) accuracyRecord[1]) + "%, ");
						}

						edu.purdue.simulation.BlockStorageSimulator.log("");
					}

					System.out.println("\nFeedback learning average acuracy :"
							+ (total_accuracy / count_accuracyRecords)
							+ ", #created models: "
							+ BlockStorageSimulator.feedbackAccuracy.size()
							+ ", # accuracy records: " + count_accuracyRecords);
				}

				if (Scheduler.isTraining == false)

					System.out.println("training dataset from experiment exp#"
							+ Scheduler.trainingExperimentID);
				/*
				 * edu.purdue.simulation.BlockStorageSimulator.log("sum: " +
				 * Scheduler.sum + " randGeneratedNumbers " +
				 * Scheduler.randGeneratedNumbers + " mean: " + (Scheduler.sum /
				 * Scheduler.randGeneratedNumbers));
				 */

				edu.purdue.simulation.BlockStorageSimulator.log("Clock: "
						+ Experiment.clock);

				// duration

				edu.purdue.simulation.BlockStorageSimulator.log("End "
						+ DateTime.now() + " duration: " + simulation_duration //
						+ " seconds");

				BlockStorageSimulator.logWrite.close();

			} catch (Exception ex) {
				edu.purdue.simulation.BlockStorageSimulator.log(ex,
						"main-finally");
			}
		}
	}

	public static void applyCommandArguments(String[] args) throws Exception {

		if (args.length > 0)

			BlockStorageSimulator.hasCommandArgs = true;

		for (int i = 0; i < args.length; i++) {
			if (args[i].startsWith("-")) {

				String commandValue = args[i + 1];

				switch (args[i]) {
				case "-isTraining":

					Scheduler.isTraining = Boolean.parseBoolean(commandValue);

					break;

				case "-trainingExperimentID":

					Scheduler.trainingExperimentID = Integer
							.parseInt(commandValue);

					break;

				case "-assessmentPolicy":
					Scheduler.assessmentPolicy = AssessmentPolicy
							.parse(commandValue);

					break;

				case "-machineLearningAlgorithm":

					Scheduler.machineLearningAlgorithm = MachineLearningAlgorithm
							.parse(commandValue);

					break;

				case "-feedBackLearning":

					Scheduler.feedBackLearning = Boolean
							.parseBoolean(commandValue);

					break;

				case "-feedbackLearningInterval":

					Scheduler.feedbackLearningInterval = Integer
							.parseInt(commandValue);

					break;

				case "-updateLearning_MinNumberOfRecords":

					Scheduler.updateLearning_MinNumberOfRecords = Integer
							.parseInt(commandValue);

					break;

				case "-updateLearning_MaxNumberOfRecords":

					Scheduler.updateLearning_MaxNumberOfRecords = Integer
							.parseInt(commandValue);

					break;

				case "-modClockBy":

					Scheduler.modClockBy = Integer.parseInt(commandValue);

					break;

				case "-StochasticEventGenerator.clockGapProbability":

					StochasticEventGenerator.clockGapProbability = Integer
							.parseInt(commandValue);

					break;

				case "-ResourceMonitor.clockGapProbability":

					ResourceMonitor.clockGapProbability = Double
							.parseDouble(commandValue);

					break;

				case "-workloadID":

					Experiment.workloadID = Integer.parseInt(commandValue);

					break;

				case "-devideDeleteFactorBy":

					Workload.devideDeleteFactorBy = Double
							.parseDouble(commandValue);

					break;

				case "-maxRequest":

					Scheduler.maxRequest = Integer.parseInt(commandValue);

					break;

				case "-startTestDatasetFromRecordRank":

					Scheduler.startTestDatasetFromRecordRank = Integer
							.parseInt(commandValue);

					break;

				case "-violationGroups":

					Scheduler.violationGroups = commandValue;

					break;

				case "-enableVolumePerformanceMeter":

					ResourceMonitor.enableVolumePerformanceMeter = Boolean
							.parseBoolean(commandValue);

					break;

				case "-saveResultPath":

					Experiment.saveResultPath = commandValue;

					break;

				case "-minRequests":

					Scheduler.minRequests = Integer.parseInt(commandValue);

					break;

				case "-recordVolumePerformanceForClocksWithNoVolume":

					ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = Boolean
							.parseBoolean(commandValue);

					break;

				default:

					throw new Exception("Unrecognized command");
				}
			}
		}
	}

	public static void log(String input) {

		if (input.startsWith("["))

			return;

		// if(BlockStorageSimulator.logWrite != null)

		BlockStorageSimulator.logWrite.println(input);

		if (!BlockStorageSimulator.hasCommandArgs)

			System.out.println(input);
	}

	public static void log(Exception ex, String blockName) {

		BlockStorageSimulator.log("\n***ERROR[" + blockName + "]***");

		BlockStorageSimulator.log(ex.getMessage());

		BlockStorageSimulator.log("\n    ~~~ Stack Trace~~~~:");

		BlockStorageSimulator
				.log(org.apache.commons.lang3.exception.ExceptionUtils
						.getStackTrace(ex));

		BlockStorageSimulator.log("***END-ERROR[" + blockName + "]***");
	}

	public static void recordReport(Experiment ex, long simulation_duration)
			throws Exception {

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
				+ "update_Learning_Min_Number_Of_Records, update_Learning_Max_Number_Of_Records, "
				+ "simulation_duration) VALUES ( " //
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
				+ "?," // update_Learning_Max_Number_Of_Records
				+ "? " // simulation_duration
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

		statement.setLong(30, simulation_duration); // simulation_duration

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

	public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
		long diffInMillies = date2.getTime() - date1.getTime();
		return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
	}
}
