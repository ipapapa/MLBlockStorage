package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.Workload;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;
import java.math.BigDecimal;

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

	public static int maxClock = 0;

	public static int schedulePausePoissonMean = 0;

	public static int devideVolumeDeleteProbability = 1;

	public static boolean considerIOPS = false;

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

	protected void preRun() throws SQLException {
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

	public void run() throws SQLException {

		this.preRun();

		StochasticEventGenerator eventGenerator = new StochasticEventGenerator();

		ResourceMonitor resourceMonitor = new ResourceMonitor();

		@SuppressWarnings("unused")
		Thread eventGeneratorThread = new Thread(eventGenerator);

		// eventGeneratorThread.start();

		@SuppressWarnings("unused")
		Thread ResourceMonitorThread = new Thread(resourceMonitor);

		// ResourceMonitorThread.start();

		BigDecimal numOne = new BigDecimal(1);

		int i = 0;

		int pauseTime = 0;

		int pauseTimer = 0;

		while (true) {

			if (i == Scheduler.maxClock)

				break;

			i++;

			eventGenerator.run();

			this.deleteExpiredVolumes();

			if (pauseTime == pauseTimer) {

				pauseTime = getPoissonRandom(Scheduler.schedulePausePoissonMean);
				pauseTimer = -1; // in case we get 0 in pauseTime

				if (this.getRequestQueue().isEmpty()) {

					// break;
				} else {

					this.schedule();
				}
			}

			resourceMonitor.run();

			pauseTimer++;

			edu.purdue.simulation.Experiment.clock = edu.purdue.simulation.Experiment.clock
					.add(numOne);

			// sum.add(new BigInteger("1"));
			//
			// if (sum.compareTo(new BigInteger("100")) == 0)
			//
			// break;
		}

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

		for (int i = 0; i < edu.purdue.simulation.Experiment.backEndList.size(); i++) {
			Backend backend = edu.purdue.simulation.Experiment.backEndList
					.get(i);

			for (int j = 0; j < backend.getVolumeList().size(); j++) {
				Volume volume = backend.getVolumeList().get(j);

				randGeneratedNumbers++;

				double randomValue = this.random.nextDouble();

				sum += randomValue;

				double deleteProbability = volume.getSpecifications().deleteProbability
						/ Scheduler.devideVolumeDeleteProbability;

				// deleteProbability = 0.2;

				if (randomValue < deleteProbability) {
					volume.delete();

					System.out.println("[DELETED VOLUME] "
							+ volume.toString(randomValue, deleteProbability));
				}
			}
		}
	}

	public abstract String getName();

	public abstract void schedule() throws SQLException;
}
