package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;

import edu.purdue.simulation.BlockStorageSimulator;
import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.Workload;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEvent;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;

import java.io.File;
import java.math.BigDecimal;

import org.apache.commons.math3.analysis.function.Exp;

import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

//import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class Scheduler {

	public Scheduler(Experiment experiment, Workload workload) throws Exception {

		this.setExperiment(experiment);

		this.setWorkload(workload);

		experiment
				.setSchedulerAlgorithm((experiment.getSchedulerAlgorithm() + " ")
						.trim() + this.getName());

		experiment.update();

		// populate scheduler queue
		this.setRequestQueue(new LinkedList<VolumeRequest>(this.Workload
				.getVolumeRequestList()));
	}

	public static MachineLearningAlgorithm machineLearningAlgorithm = MachineLearningAlgorithm.J48;

	public static Integer trainingExperimentID;

	public static int modClockBy = 1140;

	public static String violationGroups;

	public static int maxRequest = 0;

	/*
	 * it is very important to not use the training/validate dataset for
	 * experiment so this will skip first n records in the workload
	 */
	public static int startTestDatasetFromRecordRank = 10000;

	public static int minRequests = 0;

	public static int schedulePausePoissonMean = 0;

	public static boolean feedBackLearning = false;

	// interval to recreate classifiers
	public static int feedbackLearningInterval = 200;

	public static int updateLearning_MinNumberOfRecords = 1000;

	public static int updateLearning_MaxNumberOfRecords = 2000;

	public static AssessmentPolicy assessmentPolicy = AssessmentPolicy.EfficiencyFirst;

	// public static boolean considerIOPS = false;

	public static boolean isTraining = false;

	private Experiment experiment;

	private Workload Workload;

	private LinkedList<VolumeRequest> requestQueue;

	public Experiment getExperiment() {
		return experiment;
	}

	public void setExperiment(Experiment experiment) {
		this.experiment = experiment;
	}

	public Workload getWorkload() {
		return Workload;
	}

	public void setWorkload(Workload workload) {
		Workload = workload;
	}

	public LinkedList<VolumeRequest> getRequestQueue() {
		return this.requestQueue;
	}

	public void setRequestQueue(LinkedList<VolumeRequest> requestQueue) {
		this.requestQueue = requestQueue;
	}

	@SuppressWarnings("unused")
	protected double validateWithClassifier(Backend backend,
			VolumeSpecifications volumeSpecifications,
			MachineLearningAlgorithm classifier) throws Exception {

		DenseInstance instance = new DenseInstance(4);

		int clock = Experiment.clock.intValue();

		int backend_VolumesCount = backend.getVolumeList().size();

		// @attribute vio {v1,v2,v3,v4} //index 0
		// @attribute clock numeric //index 1
		// @attribute num numeric //index 2
		// @attribute tot numeric //index 3 // total allocated IOPS

		instance.setValue(1, clock % Scheduler.modClockBy);
		instance.setValue(2, backend_VolumesCount + 1);
		instance.setValue(3,
				backend.getAllocatedIOPS() + volumeSpecifications.getIOPS());

		Instances ii = Experiment.createWekaDataset(0);

		instance.setDataset(ii);

		double result = 0;

		double[] predictors = null;

		if (classifier == MachineLearningAlgorithm.J48)

			predictors = backend.j48.distributionForInstance(instance);

		else if (classifier == MachineLearningAlgorithm.BayesianNetwork)

			predictors = backend.bayesianNetwork
					.distributionForInstance(instance);

		else

			throw new Exception("cannot validate with this classifier: "
					+ classifier);

		/*
		 * predictors[0]: the probability of being in group V1
		 */

		double[] compareTo;

		switch (Scheduler.assessmentPolicy) {
		case StrictQoS:

			compareTo = new double[] { 0.99, 0.95 };
			// backend_VolumesCount
			if (BlockStorageSimulator.assessmentPolicyRules
					.containsKey(Scheduler.assessmentPolicy) == false) {
				BlockStorageSimulator.assessmentPolicyRules.put(
						Scheduler.assessmentPolicy, "predictors[0] > "
								+ compareTo[0] + " || predictors[1] > "
								+ compareTo[1]);
			}

			if (predictors[0] > compareTo[0] || predictors[1] > compareTo[1]) {

				result = predictors[0] + predictors[1];
			} else {
				int q2 = 1;
			}

			break;

		case QoSFirst:

			compareTo = new double[] { 0.99, 0.49 };

			if (BlockStorageSimulator.assessmentPolicyRules
					.containsKey(Scheduler.assessmentPolicy) == false) {
				BlockStorageSimulator.assessmentPolicyRules.put(
						Scheduler.assessmentPolicy, "predictors[0] > "
								+ compareTo[0] + " || predictors[1] > "
								+ compareTo[1]//
				);
			}

			if (predictors[0] > compareTo[0] || predictors[1] > compareTo[1]) {

				result = predictors[0] + predictors[1];

			} else {

				int v3 = 1;
			}

			break;

		case EfficiencyFirst:

			compareTo = new double[] { 0.95, 0.95, 0.98 };

			if (BlockStorageSimulator.assessmentPolicyRules
					.containsKey(Scheduler.assessmentPolicy) == false) {
				BlockStorageSimulator.assessmentPolicyRules.put(
						Scheduler.assessmentPolicy, "predictors[0]>"
								+ compareTo[0] + "||predictors[1]>"
								+ compareTo[1] + "||predictors[2]>"
								+ compareTo[2]);
			}

			if (predictors[0] > compareTo[0] || predictors[1] > compareTo[1]
					|| predictors[2] > compareTo[2]) {

				result = predictors[0] + predictors[1] + predictors[2];

			} else {
				int v3 = 1;
			}

			break;

		case MaxEfficiency:

			compareTo = new double[] { 0.6, 0.6, 0.6 };

			if (BlockStorageSimulator.assessmentPolicyRules
					.containsKey(Scheduler.assessmentPolicy) == false) {
				BlockStorageSimulator.assessmentPolicyRules.put(
						Scheduler.assessmentPolicy, "predictors[0]>"
								+ compareTo[0] + "||predictors[1]>"
								+ compareTo[1] + "||predictors[2]>"
								+ compareTo[2]);
			}

			if (predictors[0] > compareTo[0] || predictors[1] > compareTo[1]
					|| predictors[2] > compareTo[2]) {
				/*
				 * predictors[2] must be used here because we are not interested
				 * in the probability of having V4 (high violations)
				 */
				result = predictors[0] + predictors[1] + predictors[2];

			} else {
				int q5 = 1;
			}

			break;
		}

		return result;
	}

	protected void sortBackendListBaseOnAvailableCapacity() {
		Collections.sort(edu.purdue.simulation.Experiment.backendList,
				new Comparator<Backend>() {
					@Override
					public int compare(Backend backEnd1, Backend backEnd2) {
						return Integer.compare(backEnd2.getState()
								.getAvailableCapacity(), backEnd1.getState()
								.getAvailableCapacity());
					}
				});
	}

	/*
	 * bad function delete it. not used and broken
	 */
	protected ScheduleResponse.RejectionReason validateResources(
			Backend backend, VolumeSpecifications volumeRequestSpecifications,
			MachineLearningAlgorithm machineLearningAlgorithm) throws Exception {

		ScheduleResponse.RejectionReason result = ScheduleResponse.RejectionReason.none;

		boolean validateIOPS = false;

		/*
		 * Do not consider IOPS in training
		 */
		if (Scheduler.isTraining) {
			validateIOPS = true;

		} else {

			// validateIOPS =
			this.validateWithClassifier(backend, volumeRequestSpecifications,
					machineLearningAlgorithm);
		}

		boolean validateCapacity = true;

		if (backend.getState().getAvailableCapacity() < volumeRequestSpecifications
				.getCapacity()) {

			validateCapacity = false;
		}

		if (!validateIOPS)

			result = ScheduleResponse.RejectionReason.IOPS;

		if (!validateCapacity)

			result = ScheduleResponse.RejectionReason.Capacity;

		if (!validateIOPS && !validateCapacity)

			result = ScheduleResponse.RejectionReason.IOPS_Capacity;

		// TODO check the available IOPS, print injecetion reason\

		// int avail =
		// maxAvailableCapacityBackEnd.getAvailableIOPSWithRegression();

		// if (!Scheduler.considerIOPS || avail > requestedIOPS) {
		// volume = maxAvailableCapacityBackEnd.createVolumeThenAdd(
		// requestedSpecifications, schedulerResponse);
		// }

		return result;

	}

	protected void preRun() throws Exception {
		throw new UnsupportedOperationException();
	}

	protected BackEndSpecifications currentExpectedSpecifications_Regression(
			Backend backend) {

		// TODO call the database and do regression on current clock
		BackEndSpecifications result = new BackEndSpecifications();

		result.setIOPS(700);

		return result;
	}

	protected BackendCategories getBackendCategory(Backend backend) {

		// TODO STD AVG

		return BackendCategories.Unstable;
	}

	protected int getExpectedIOPSBasedOnBackendStability(Backend backend) {

		BackEndSpecifications specifications = this
				.currentExpectedSpecifications_Regression(backend);

		// Remmeber you have to automate category by looking at mean and std of
		// backend
		switch (this.getBackendCategory(backend)) {
		case VeryStable:

			return (int) (specifications.getIOPS() * 0.1);

		case Stable:

			return (int) (specifications.getIOPS() * 0.2);

		case Unstable:

			return (int) (specifications.getIOPS() * 0.4);

		case VeryUnstable:

			return (int) (specifications.getIOPS() * 0.5);

		default:
			break;
		}

		return 0;
	}

	public static int getPoissonRandom(double mean) {
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

	private LinkedList<String> validateConfigurationParameres() {

		LinkedList<String> result = new LinkedList<String>();

		if (Scheduler.maxRequest <= Scheduler.minRequests) {

			result.add("Scheduler.maxClock must be bigger than Scheduler.minRequests");
		}

		File folder = new File(Experiment.saveResultPath);

		if (folder.exists() == false)

			result.add("Experiment.saveResultPath - path does not exists - "
					+ Experiment.saveResultPath);

		return result;
	}

	public void run() throws Exception {

		// #1: initialize the clock to the first request arrival time
		edu.purdue.simulation.Experiment.clock = new BigDecimal(this
				.getRequestQueue().peek().getArrivalTime());

		LinkedList<String> validationResult = this
				.validateConfigurationParameres();

		if (validationResult.size() > 0) {

			String exceptionMessage = "";

			for (String err : validationResult) {
				exceptionMessage += err + "\n\n";
			}

			throw new Exception(exceptionMessage);
		}

		this.preRun();

		StochasticEventGenerator eventGenerator = new StochasticEventGenerator();

		ResourceMonitor resourceMonitor = new ResourceMonitor();

		BigDecimal numOne = new BigDecimal(1);

		int requestNumber = 0;

		// int queueInitialSize = this.getRequestQueue().size();

		int clockIntValue = 0;

		int currentClockRequests = 0;

		while (true) {

			clockIntValue = edu.purdue.simulation.Experiment.clock.intValue();

			if (this.getRequestQueue().size() == 0)

				break;

			if (requestNumber >= Scheduler.maxRequest) {

				break;

				// if (Scheduler.minRequests == 0) {
				//
				// break;
				// }
				// else if ((queueInitialSize - this.getRequestQueue().size())
				// >= Scheduler.minRequests) {
				//
				// break;
				// }
			}

			/*
			 * having multiple requests at a same clock, which means a clock
			 * will increase if there is no more requests for that clock. Most
			 * functions must be done when the clock is increases. For example,
			 * the resource evaluation process. The sequence of instructions in
			 * this block of code is important.
			 */
			int nextRequestClock = this.getRequestQueue().peek()
					.getArrivalTime();
			boolean goToNextClock = nextRequestClock != (clockIntValue % Scheduler.modClockBy);
			/*
			 * first delete expired volumes before schedule a new request
			 */
			if (goToNextClock) {
				this.deleteExpiredVolumes();
			}

			VolumeRequest volumeRequest = this.getRequestQueue().peek();

			int arrivalTime = volumeRequest.getArrivalTime();

			// if (pauseTime == pauseTimer) {
			if (arrivalTime == (clockIntValue % Scheduler.modClockBy)) {

				if (this.getRequestQueue().isEmpty()) {

					// break;
				} else {

					// if (currentClockRequests < 4)

					this.schedule(volumeRequest);

					this.getRequestQueue().remove();

					requestNumber++;
				}
			}

			if (goToNextClock) {

				eventGenerator.run();

				this.deleteExpiredVolumes();

				resourceMonitor.run();

				/*
				 * must save changes in db before feedback learning
				 */
				Database.executeBatchQuery(ResourceMonitor.backendStat_queries,
						false);

				Database.executeBatchQuery(VolumePerformanceMeter.queries,
						false);

				Database.executeBatchQuery(StochasticEvent.queries, false);

				/*
				 * applies feedback learning
				 */
				if (Scheduler.feedBackLearning
						&& clockIntValue > 0
						&& (clockIntValue % Scheduler.feedbackLearningInterval == 0)) {

					/*
					 * having multiple requests in a clock will cause creating
					 * models multiple times. I prefer to make the model based
					 * on the first request of each click
					 */
					if (BlockStorageSimulator.feedbackAccuracy
							.containsKey(clockIntValue) == false) {

						BlockStorageSimulator.feedbackAccuracy.put(
								clockIntValue,
								new Object[Experiment.backendList.size()][2]);

						this.experiment.createUpdateTrainingData(
						//
								0, // numberOfRecords
								this.getExperiment(), // experiment
								null, // path
								0, // includeViolationsNumber
								Scheduler.updateLearning_MaxNumberOfRecords); // updateLearningModelByLastNumberOfRecords
					}
				}

				/*
				 * clock must be increased after all statements/processes are
				 * done
				 */
				edu.purdue.simulation.Experiment.clock = edu.purdue.simulation.Experiment.clock
						.add(numOne);

				currentClockRequests = 0;
			}

			currentClockRequests++;
		}

		Database.executeBatchQuery(ResourceMonitor.backendStat_queries, true);

		Database.executeBatchQuery(StochasticEvent.queries, true);

		Database.executeBatchQuery(VolumePerformanceMeter.queries, true);

		// for (i = 0; i < Experiment.backendList.size(); i++) {
		//
		// Backend.createTrainingDataForRepTree(0, this.getExperiment(),
		// Experiment.backendList.get(i), null);
		//
		// }

		if (Scheduler.isTraining)

			this.experiment.createUpdateTrainingData(0, this.getExperiment(),
					null, 0, //
					0 // no feedback learning/update model
					);

		else

			this.experiment.createUpdateTrainingData(0, this.getExperiment(),
					null, 1, //
					0 // no feedback learning/update model
					); // include all values

		// }

		// eventGeneratorThread.interrupt();
		// ResourceMonitorThread.interrupt();
		//
		// edu.purdue.simulation.BlockStorageSimulator.log("DONE - with threads");
	}

	public static double sum = 0;

	public static double randGeneratedNumbers = 0;

	private void deleteExpiredVolumes() throws SQLException, Exception {
		//
		// if (Experiment.clock.intValue() > 450) {
		// edu.purdue.simulation.BlockStorageSimulator.log();
		//
		// }

		int currentClock = Experiment.clock.intValue();

		for (int i = 0; i < edu.purdue.simulation.Experiment.backendList.size(); i++) {
			Backend backend = edu.purdue.simulation.Experiment.backendList
					.get(i);

			for (int j = 0; j < backend.getVolumeList().size(); j++) {
				Volume volume = backend.getVolumeList().get(j);

				randGeneratedNumbers++;

				/*
				 * double randomValue = this.random.nextDouble();
				 * 
				 * sum += randomValue;
				 * 
				 * double deleteProbability =
				 * volume.getSpecifications().deleteFactor /
				 * Scheduler.devideVolumeDeleteProbability;
				 * 
				 * // deleteProbability = 0.2;
				 */

				VolumeSpecifications volumeSpec = volume.getSpecifications();

				int deleteFactor = (int) (volumeSpec.deleteFactor + volumeSpec.createClock);

				// if (randomValue < deleteProbability) {
				if (deleteFactor <= currentClock) {
					volume.delete();

					edu.purdue.simulation.BlockStorageSimulator
							.log("[DELETED VOLUME] vol_ID = "
									+ volume.getID().toString()//
									+ "del_prob = "//
									+ deleteFactor);
				}
			}
		}
	}

	public abstract String getName();

	public abstract void schedule(VolumeRequest volumeRequest) throws Exception;
}
