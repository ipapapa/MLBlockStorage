package edu.purdue.simulation.blockstorage;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;

public class MaxCapacityFirstScheduler extends Scheduler {

	public MaxCapacityFirstScheduler(
			edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);
	}

	protected void preRun() throws Exception {

		/*
		 * Each node contains 18 * 2TB 7.2K Serial Attached SCSI (SAS) within a
		 * Redundant Array of Independent Disk (RAID)
		 */
		int capacity = 36000;

		/*
		 * Each hard drive can achieve up to 190 IOPS throughput.
		 */
		int bandwidth = 2280;

		int maxBandwidth = 2800;

		int minBandwidth = 1200;

		MachineLearningAlgorithm machineLearningAlgorithm = Scheduler.machineLearningAlgorithm;

		String path = Experiment.saveResultPath;

		File folder = new File(path);

		if (folder.exists() == false)

			throw new Exception(
					"Experiment.saveResultPath - path does not exists - "
							+ path);

		File[] listOfFiles = folder.listFiles();

		String[] backends = new String[6];

		int j = 0;

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String fileName = listOfFiles[i].getName();

				if (fileName.startsWith("TRN_"
						+ Scheduler.trainingExperimentID.toString())) {
					backends[j] = path + fileName;

					j++;
				}
			}
		}

		super.getExperiment().addBackEnd("B1", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				200, // stabilityPossessionMean
				backends[0],//
				machineLearningAlgorithm));

		super.getExperiment().addBackEnd("B2", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				300, // stabilityPossessionMean
				backends[1],//
				machineLearningAlgorithm));

		super.getExperiment().addBackEnd("B3", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				400, // stabilityPossessionMean
				backends[2],//
				machineLearningAlgorithm));

		super.getExperiment().addBackEnd("B4", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				500, // stabilityPossessionMean
				backends[3],//
				machineLearningAlgorithm));

		super.getExperiment().addBackEnd("B5", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				100, // stabilityPossessionMean
				backends[4],//
				machineLearningAlgorithm));

		super.getExperiment().addBackEnd("B6", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				600, // stabilityPossessionMean
				backends[5], //
				machineLearningAlgorithm));
	}

	private class WeighteningVector {

		public WeighteningVector(Backend backend, int availableCapacity,
				double IOPS_ProbabilityThatSatisfyAssessmentPolicy) {
			super();
			this.backend = backend;
			this.availableCapacity = availableCapacity;
			this.IOPS_ProbabilityThatSatisfyAssessmentPolicy = IOPS_ProbabilityThatSatisfyAssessmentPolicy;
		}

		public Backend backend;

		public int availableCapacity;

		public double IOPS_ProbabilityThatSatisfyAssessmentPolicy;

		@Override
		public String toString() {
			return this.backend.toString();
		}
	}

	private Backend filtering_Weightning_Backends(VolumeRequest volumeRequest) {

		ArrayList<WeighteningVector> candidateBackends = new ArrayList<WeighteningVector>();

		VolumeSpecifications requestSpecifications = volumeRequest
				.ToVolumeSpecifications();

		for (Backend b : edu.purdue.simulation.Experiment.backendList) {
			
			boolean satisfy = true;

			int availableCapacity = b.getState().getAvailableCapacity();

			if (!(availableCapacity > volumeRequest.getCapacity()))

				satisfy = satisfy && false;

			double IOPS_ProbabilityThatSatisfyAssessmentPolicy = 0;

			if (!Scheduler.isTraining && satisfy) {

				IOPS_ProbabilityThatSatisfyAssessmentPolicy = this
						.validateWithClassifier(b, requestSpecifications,
								machineLearningAlgorithm);

				if (!(IOPS_ProbabilityThatSatisfyAssessmentPolicy > 0))

					satisfy = satisfy && false;
			}

			if (satisfy)

				candidateBackends.add(new WeighteningVector(b,
						availableCapacity,
						IOPS_ProbabilityThatSatisfyAssessmentPolicy));
		}

		/*
		 * Weighting
		 */
		Collections.sort(candidateBackends,
				new Comparator<WeighteningVector>() {
					@Override
					public int compare(WeighteningVector vector1,
							WeighteningVector vector2) {

						int c;

						c = Double
								.compare(
										vector2.IOPS_ProbabilityThatSatisfyAssessmentPolicy,
										vector1.IOPS_ProbabilityThatSatisfyAssessmentPolicy);

						if (c == 0)

							c = Integer.compare(vector2.availableCapacity,
									vector1.availableCapacity);

						return c;
					}
				});

		if (candidateBackends.size() == 0)

			return null;

		return candidateBackends.get(0).backend;
	}

	public void schedule(VolumeRequest volumeRequest)
			throws java.lang.Exception {

		Backend bestCandidateBackend = filtering_Weightning_Backends(volumeRequest);

		ScheduleResponse schedulerResponse = new ScheduleResponse( //
				this.getExperiment(), //
				volumeRequest);

		Volume volume = null;

		VolumeSpecifications requestedSpecifications = volumeRequest
				.ToVolumeSpecifications();

		/*
		 * Scheduler.isTraining if is true -> only capacity will be checked
		 */
		if (bestCandidateBackend != null) {

			volume = bestCandidateBackend.createVolumeThenAdd(
					requestedSpecifications, schedulerResponse);
		}

		/*
		 * there is no volume available. either reject or create new volume in
		 * case of reject a SchedulerResponse record will be saved
		 */
		if (volume == null) {

			schedulerResponse.isSuccessful = false;

			schedulerResponse.backEndScheduled = null;

			/*
			 * You can dynamically add backends here if needed, or just reject
			 * the volume
			 */
			// schedulerResponse.backEndCreated = super.getExperiment()
			// .AddBackEnd(this.specifications); // no backend created

			schedulerResponse.backEndTurnedOn = null; // no backend turned on

			System.out.println("[Failed to schedule] -> "
					+ volumeRequest.toString());
		} else {

			schedulerResponse.isSuccessful = true;

			schedulerResponse.backEndScheduled = bestCandidateBackend;

			System.out.println("[Successfully scheduled] ->"
					+ volumeRequest.toString() + " backendID= "
					+ schedulerResponse.backEndScheduled.getID());
		}

		/*
		 * first save the schedule response then the volume
		 */
		schedulerResponse.save();

		if (volume != null)

			volume.save();
	}

	/**
	 * @param backend
	 * @param volumeRequestSpecifications
	 * @param method
	 *            1 is RepTree
	 * @return
	 */

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "MaxCapacityFirstScheduler";
	}

}
