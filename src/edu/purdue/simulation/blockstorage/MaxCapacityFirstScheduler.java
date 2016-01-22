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
			edu.purdue.simulation.Workload workload) throws Exception {
		super(experiment, workload);
	}

	protected void preRun() throws Exception {

		/*
		 * Each node contains 18 * 2TB 7.2K Serial Attached SCSI (SAS) within a
		 * Redundant Array of Independent Disk (RAID)
		 */

		int harddriveCount = 7;

		int capacity = harddriveCount * 2000; // =14000

		/*
		 * Each hard drive can achieve up to 190 IOPS throughput.    3 * 450 = 1350
		 */
		int bandwidth = harddriveCount * 200; // = 1400 

		int maxBandwidth = 2300;

		int minBandwidth = 700;

		MachineLearningAlgorithm machineLearningAlgorithm = Scheduler.machineLearningAlgorithm;

		String path = Experiment.saveResultPath;

		File folder = new File(path);

		if (folder.exists() == false)

			throw new Exception(
					"Experiment.saveResultPath - path does not exists - "
							+ path);

		File[] listOfFiles = folder.listFiles();

		String[] trainingWorkloadPath = new String[6];

		int j = 0;

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String fileName = listOfFiles[i].getName();

				if (fileName.startsWith("TRN_"
						+ Scheduler.trainingExperimentID.toString())) {
					trainingWorkloadPath[j] = path + fileName;

					j++;
				}
			}
		}

		double[] stabilityPMean = new double[] {//
		350, 300, 450, 200, 500, 600 };

		super.getExperiment().addBackEnd("B1", new BackEndSpecifications(//
				capacity, // Capacity
				capacity,// Max Capacity
				capacity, // Min capacity
				bandwidth, // IOPS
				maxBandwidth, // Max IOPS
				minBandwidth, // Min IOPS
				0, // Latency
				true, // is online
				stabilityPMean[0], // stabilityPossessionMean
				trainingWorkloadPath[0],// traing path
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
				stabilityPMean[1], // stabilityPossessionMean
				trainingWorkloadPath[1],//
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
				stabilityPMean[2], // stabilityPossessionMean
				trainingWorkloadPath[2],//
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
				stabilityPMean[3], // stabilityPossessionMean
				trainingWorkloadPath[3],//
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
				stabilityPMean[4], // stabilityPossessionMean
				trainingWorkloadPath[4],//
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
				stabilityPMean[5], // stabilityPossessionMean
				trainingWorkloadPath[5], //
				machineLearningAlgorithm));
	}

	private class _Vector_Weightening {

		public _Vector_Weightening(Backend backend, int availableCapacity,
				double IOPS_ProbabilityThatSatisfyAssessmentPolicy) {
			super();
			this.backend = backend;
			this.availableCapacity = availableCapacity;
			this.IOPS_ProbabilityThatSatisfyAssessmentPolicy = IOPS_ProbabilityThatSatisfyAssessmentPolicy;

			this.allocatedIOPS = backend.getAllocatedIOPS();
		}

		public Backend backend;

		public int availableCapacity;

		public int allocatedIOPS;

		public double IOPS_ProbabilityThatSatisfyAssessmentPolicy;

		@Override
		public String toString() {
			return "allocated IOPS: " //
					+ this.allocatedIOPS //
					+ "#vols: " //
					+ this.backend.getVolumeList().size() //
					+ " IOPS_Prob: " //
					+ this.IOPS_ProbabilityThatSatisfyAssessmentPolicy //
					+ " availableCapacity: " //
					+ this.availableCapacity //
					+ "~~Backend ->" + //
					this.backend.toString();
		}
	}

	@SuppressWarnings("unused")
	private Backend filtering_Weightning_Backends(VolumeRequest volumeRequest)
			throws Exception {

		ArrayList<_Vector_Weightening> candidateBackends = new ArrayList<_Vector_Weightening>();

		VolumeSpecifications requestSpecifications = volumeRequest
				.ToVolumeSpecifications();

		if (Experiment.clock.intValue() >= 222) {
			int q1 = 1;
		}

		for (Backend b : edu.purdue.simulation.Experiment.backendList) {

			int baackend_vol_count = b.getVolumeList().size();

			boolean satisfy = true;

			int availableCapacity = b.getState().getAvailableCapacity();

			if (!(availableCapacity > volumeRequest.getCapacity())) {

				satisfy = satisfy && false;

				if (baackend_vol_count == 0) {
					int q2 = 1;
				}
			}

			double IOPS_ProbabilityThatSatisfyAssessmentPolicy = 0;

			if (!Scheduler.isTraining && satisfy) {

				IOPS_ProbabilityThatSatisfyAssessmentPolicy = this
						.validateWithClassifier(b, requestSpecifications,
								machineLearningAlgorithm);

				if (!(IOPS_ProbabilityThatSatisfyAssessmentPolicy > 0)) {

					satisfy = satisfy && false;

					if (baackend_vol_count == 0) {
						int q2 = 1;
					}
				}
			}

			if (satisfy)

				candidateBackends.add(new _Vector_Weightening(b,
						availableCapacity,
						IOPS_ProbabilityThatSatisfyAssessmentPolicy));
		}
		// candidateBackends.get(0).backend.getVolumeList().size()
		/*
		 * Weighting
		 */
		Collections.sort(candidateBackends,
				new Comparator<_Vector_Weightening>() {
					@Override
					public int compare(_Vector_Weightening vector1,
							_Vector_Weightening vector2) {

						int c;

						c = Double.compare(vector1.allocatedIOPS,
								vector2.allocatedIOPS);

						if (c == 0)

							c = Integer.compare(vector2.availableCapacity,
									vector1.availableCapacity);

						return c;
					}
				});

		if (candidateBackends.size() == 0)

			return null;

		Backend result = candidateBackends.get(0).backend;

		if (result.getVolumeList().size() > 2) {
			int ww = result.getVolumeList().size();
		}// Experiment.clock
			// Experiment.backendList
		return result;
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

			edu.purdue.simulation.BlockStorageSimulator
					.log("[Failed to schedule] -> " + volumeRequest.toString());
		} else {

			schedulerResponse.isSuccessful = true;

			schedulerResponse.backEndScheduled = bestCandidateBackend;

			edu.purdue.simulation.BlockStorageSimulator
					.log("[Successfully scheduled] ->"
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
