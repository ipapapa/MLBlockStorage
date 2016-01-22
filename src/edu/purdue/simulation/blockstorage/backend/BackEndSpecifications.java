package edu.purdue.simulation.blockstorage.backend;

import edu.purdue.simulation.blockstorage.*;

public class BackEndSpecifications extends Specifications {

	public BackEndSpecifications() {

		super();

	}

	public BackEndSpecifications(int capacity, int maxCapacity,
			int minCapacity, int IOPS, int maxIOPS, int minIOPS, int latency,
			boolean isOnline, double stabilityPossessionMean, String trainingWorkloadPath,
			MachineLearningAlgorithm machineLearningAlgorithm) {
		super(capacity, IOPS, latency);

		this.setMaxIOPS(maxIOPS);
		this.setMinIOPS(minIOPS);

		this.setMaxCapacity(maxCapacity);
		this.setMinCapacity(minCapacity);

		this.setLatency(latency);

		this.setStabilityPossessionMean(stabilityPossessionMean);

		this.setTrainingWorkloadPath(trainingWorkloadPath);

		this.setMachineLearningAlgorithm(machineLearningAlgorithm);
	}

	private double stabilityPossessionMean;

	//private boolean isOnline;

	private int maxIOPS;

	private int minIOPS;

	private int maxCapacity;

	private int minCapacity;

	private String trainingWorkloadPath;

	private MachineLearningAlgorithm machineLearningAlgorithm;

	public String getTrainingDataSetPath() {
		return trainingWorkloadPath;
	}

	public void setTrainingWorkloadPath(String path) {
		this.trainingWorkloadPath = path;
	}

	public String getTrainingWorkloadPath() {
		return this.trainingWorkloadPath;
	}

	public MachineLearningAlgorithm getMachineLearningAlgorithm() {
		return machineLearningAlgorithm;
	}

	public void setMachineLearningAlgorithm(
			MachineLearningAlgorithm machineLearningAlgorithm) {
		this.machineLearningAlgorithm = machineLearningAlgorithm;
	}

	public double getStabilityPossessionMean() {
		return stabilityPossessionMean;
	}

	public void setStabilityPossessionMean(double stabilityPossessionMean) {
		this.stabilityPossessionMean = stabilityPossessionMean;
	}

	// public boolean getIsOnline() {
	// return this.isOnline;
	// }
	//
	// public void setIsOnline(boolean isOnline) {
	// this.isOnline = isOnline;
	// }

	public int getMaxIOPS() {
		return maxIOPS;
	}

	public void setMaxIOPS(int maxIOPS) {
		this.maxIOPS = maxIOPS;
	}

	public int getMinIOPS() {
		return minIOPS;
	}

	public void setMinIOPS(int minIOPS) {
		this.minIOPS = minIOPS;
	}

	public int getMaxCapacity() {
		return maxCapacity;
	}

	public void setMaxCapacity(int maxCapacity) {
		this.maxCapacity = maxCapacity;
	}

	public int getMinCapacity() {
		return minCapacity;
	}

	public void setMinCapacity(int minCapacity) {
		this.minCapacity = minCapacity;
	}
}