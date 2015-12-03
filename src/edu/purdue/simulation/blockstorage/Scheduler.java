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

	public Scheduler(Experiment experiment, Workload workload) {

		this.setExperiment(experiment);

		this.setWorkload(workload);

		experiment
				.setSchedulerAlgorithm((experiment.getSchedulerAlgorithm() + " ")
						.trim() + this.getName());

		try {
			experiment.update();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// populate scheduler queue
		this.setRequestQueue(new LinkedList<VolumeRequest>(this.Workload
				.getVolumeRequestList()));
	}

	public static MachineLearningAlgorithm machineLearningAlgorithm = MachineLearningAlgorithm.J48;

	public static Integer trainingExperimentID;

	public static int modClockBy = 1140;

	public static int maxRequest = 0;

	public static int minRequests = 0;

	public static int schedulePausePoissonMean = 0;

	public static int devideVolumeDeleteProbability = 1;

	public static boolean feedBackLearning = false;

	public static int feedBackLearningInterval = 200;

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

	protected boolean validateWithRepTree(Backend backend,
			VolumeSpecifications volumeSpecifications) {

		Instance instance = new DenseInstance(4);

		int clock = Experiment.clock.intValue();

		int backendSize = backend.getVolumeList().size();

		int totalRequestedCap = 0;

		for (int i = 0; i < backendSize; i++) {
			totalRequestedCap += backend.getVolumeList().get(i)
					.getSpecifications().getIOPS();
		}

		// @attribute vio {v1,v2,v3,v4} //index 0
		// @attribute clock numeric //index 1
		// @attribute num numeric //index 2
		// @attribute tot numeric //index 3

		instance.setValue(1, clock % Scheduler.modClockBy);
		instance.setValue(2, backendSize + 1);
		instance.setValue(3, totalRequestedCap + volumeSpecifications.getIOPS());

		try {
			double[] predictors = backend.repTree
					.distributionForInstance(instance);

			/*
			 * predictors[0]: the probability of being in group V1
			 */

			// if (predictors[0] > predictors[1] + predictors[2] +
			// predictors[3])
			// if ((predictors[1] >= predictors[2])
			// && (predictors[1] >= predictors[3])
			// //&& (predictors[0] >= predictors[3])
			// )

			// if (predictors[3] < 0.999)
			if (predictors[0] > 0.9 || predictors[0] > 0.2)

				return true;

			else

				return false;

		} catch (Exception e) {

			e.printStackTrace();

		}

		// TODO FIX THIS

		return true;
	}

	protected boolean validateWithJ48(Backend backend,
			VolumeSpecifications volumeSpecifications) {

		DenseInstance instance = new DenseInstance(4);

		int clock = Experiment.clock.intValue();

		int backendSize = backend.getVolumeList().size();

		int totalRequestedCap = 0;

		for (int i = 0; i < backendSize; i++) {
			totalRequestedCap += backend.getVolumeList().get(i)
					.getSpecifications().getIOPS();
		}

		// @attribute vio {v1,v2,v3,v4} //index 0
		// @attribute clock numeric //index 1
		// @attribute num numeric //index 2
		// @attribute tot numeric //index 3

		instance.setValue(1, clock % Scheduler.modClockBy);
		instance.setValue(2, backendSize + 1);
		instance.setValue(3, totalRequestedCap + volumeSpecifications.getIOPS());

		Instances ii = Experiment.createWekaDataset(0);

		instance.setDataset(ii);

		boolean result = false;

		try {

			double[] predictors = backend.j48.distributionForInstance(instance);

			/*
			 * predictors[0]: the probability of being in group V1
			 */

			// if (predictors[0] > predictors[1] + predictors[2] +
			// predictors[3])
			// if ((predictors[1] >= predictors[2])
			// && (predictors[1] >= predictors[3])
			// //&& (predictors[0] >= predictors[3])
			// )

			// if (predictors[3] < 0.999)

			switch (Scheduler.assessmentPolicy) {
			case StrictQoS:

				if (predictors[0] >= 0.5)

					result = true;

				break;

			case QoSFirst:

				if (predictors[0] + predictors[1] > 0.1)

					result = true;

				break;

			case EfficiencyFirst:

				if (predictors[0] + predictors[1] > 0.1 || predictors[2] == 1)

					result = true;

				break;

			case MaxEfficiency:

				if (predictors[0] + predictors[1] > 0.1 || predictors[3] != 1)

					result = true;

				break;
			}

		} catch (Exception e) {

			System.out.println(e.getMessage());

			e.printStackTrace();

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

			switch (machineLearningAlgorithm) {
			case RepTree:

				validateIOPS = validateWithRepTree(backend,
						volumeRequestSpecifications);

				break;

			case J48:

				validateIOPS = validateWithJ48(backend,
						volumeRequestSpecifications);

				break;

			default:
				throw new java.lang.Exception(
						"the validation method is not defined;");

			}

			if (machineLearningAlgorithm == MachineLearningAlgorithm.RepTree) {

			} else {

			}
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

		// @SuppressWarnings("unused")
		// Thread eventGeneratorThread = new Thread(eventGenerator);

		// eventGeneratorThread.start();

		// @SuppressWarnings("unused")
		// Thread ResourceMonitorThread = new Thread(resourceMonitor);

		// ResourceMonitorThread.start();

		BigDecimal numOne = new BigDecimal(1);

		int requestNumber = 1; // first request number is 1

		int queueInitialSize = this.getRequestQueue().size();

		int clockIntValue = 0;

		int currentClockRequests = 0;

		while (true) {

			clockIntValue = edu.purdue.simulation.Experiment.clock.intValue();

			if (requestNumber >= Scheduler.maxRequest) {

				if (Scheduler.minRequests == 0) {

					break;
				} else if ((queueInitialSize - this.getRequestQueue().size()) >= Scheduler.minRequests) {

					break;

				}
			}

			if (Scheduler.feedBackLearning && clockIntValue > 0
					&& clockIntValue % Scheduler.feedBackLearningInterval == 0) {

				BlockStorageSimulator.feedbackAccuracy.put(clockIntValue,
						new Object[Experiment.backendList.size()][2]);

				this.experiment.createUpdateTrainingDataForRepTree(//
						0, //
						this.getExperiment(), //
						null, //
						0, //
						Scheduler.feedBackLearningInterval);
			}

			eventGenerator.run();

			this.deleteExpiredVolumes();

			VolumeRequest volumeRequest = this.getRequestQueue().peek();

			int arrivalTime = volumeRequest.getArrivalTime();

			// if (pauseTime == pauseTimer) {
			if (arrivalTime == clockIntValue) {

				if (this.getRequestQueue().isEmpty()) {

					// break;
				} else {

					// if (currentClockRequests < 4)

					this.schedule(volumeRequest);

					this.getRequestQueue().remove();

					requestNumber++;
				}
			}

			resourceMonitor.run();

			// edu.purdue.simulation.Experiment.clock =
			// edu.purdue.simulation.Experiment.clock
			// .add(numOne);

			int nextRequestClock = this.getRequestQueue().peek()
					.getArrivalTime();

			if (nextRequestClock > clockIntValue) {

				edu.purdue.simulation.Experiment.clock = edu.purdue.simulation.Experiment.clock
						.add(numOne);

				currentClockRequests = 0;
			}

			currentClockRequests++;

			// sum.add(new BigInteger("1"));
			//
			// if (sum.compareTo(new BigInteger("100")) == 0)
			//
			// break;

			Database.executeBatchQuery(StochasticEvent.queries, 1000);

			Database.executeBatchQuery(VolumePerformanceMeter.queries, 1000);
		}

		Database.executeBatchQuery(StochasticEvent.queries, 0);

		Database.executeBatchQuery(VolumePerformanceMeter.queries, 0);

		// for (i = 0; i < Experiment.backendList.size(); i++) {
		//
		// Backend.createTrainingDataForRepTree(0, this.getExperiment(),
		// Experiment.backendList.get(i), null);
		//
		// }

		if (Scheduler.isTraining)

			this.experiment.createUpdateTrainingDataForRepTree(0,
					this.getExperiment(), null, 0, //
					0 // no feedback learning/update model
					);

		else

			this.experiment.createUpdateTrainingDataForRepTree(0,
					this.getExperiment(), null, 1, //
					0 // no feedback learning/update model
					); // include all values

		// }

		// eventGeneratorThread.interrupt();
		// ResourceMonitorThread.interrupt();
		//
		// System.out.println("DONE - with threads");
	}

	private Random random = new Random();

	public static double sum = 0;

	public static double randGeneratedNumbers = 0;

	private void deleteExpiredVolumes() throws SQLException {
		//
		// if (Experiment.clock.intValue() > 450) {
		// System.out.println();
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

					System.out.println("[DELETED VOLUME] "
							+ volume.toString(deleteFactor));
				}
			}
		}
	}

	public abstract String getName();

	public abstract void schedule(VolumeRequest volumeRequest) throws Exception;
}
