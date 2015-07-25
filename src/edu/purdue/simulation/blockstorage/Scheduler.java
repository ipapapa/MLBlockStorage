package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;

import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.Workload;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;
import java.math.BigDecimal;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public abstract class Scheduler {

	public Scheduler(Experiment experiment, Workload workload) {

		this.Experiment = experiment;

		this.Workload = workload;

		// populate scheduler queue
		this.RequestQueue = new LinkedList<VolumeRequest>(
				this.Workload.getVolumeRequestList());
	}

	private Experiment Experiment;

	private Workload Workload;

	private LinkedList<VolumeRequest> RequestQueue;

	public Experiment getExperiment() {
		return Experiment;
	}

	public void setExperiment(Experiment experiment) {
		Experiment = experiment;
	}

	public Workload getWorkload() {
		return Workload;
	}

	public void setWorkload(Workload workload) {
		Workload = workload;
	}

	public LinkedList<VolumeRequest> getRequestQueue() {
		return RequestQueue;
	}

	public void setRequestQueue(LinkedList<VolumeRequest> requestQueue) {
		RequestQueue = requestQueue;
	}

	protected void preRun() throws SQLException {
		throw new NotImplementedException();
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

		while (true) {

			eventGenerator.run();

			resourceMonitor.run();

			if (this.getRequestQueue().isEmpty()) {

				break;
			} else {

				this.schedule();
			}
			// System.out.println("Scheduler");

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

	public abstract void schedule() throws SQLException;
}
