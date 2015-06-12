package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.Workload;

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

	public abstract void Schedule() throws SQLException;

}
