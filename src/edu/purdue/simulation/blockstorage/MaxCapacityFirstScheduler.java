package edu.purdue.simulation.blockstorage;

import java.io.File;
import java.sql.SQLException;

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

		int capacity = 7200 * 5; // no problem with capacity

		int bandwidth = 1948;

		int maxBandwidth = bandwidth + 500;

		int minBandwidth = bandwidth - 500;

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

	public void schedule(VolumeRequest volumeRequest)
			throws java.lang.Exception {

		super.sortBackendListBaseOnAvailableCapacity();

		Backend maxAvailableCapacityBackEnd = edu.purdue.simulation.Experiment.backendList
				.get(0);

		ScheduleResponse schedulerResponse = new ScheduleResponse( //
				this.getExperiment(), //
				volumeRequest);

		VolumeSpecifications requestedSpecifications = volumeRequest
				.ToVolumeSpecifications();

		Volume volume = null;

		// int requestedIOPS = requestedSpecifications.getIOPS();

		ScheduleResponse.RejectionReason rejectionReason = super
				.validateResources(maxAvailableCapacityBackEnd,
						requestedSpecifications,
						Scheduler.machineLearningAlgorithm);

		/*
		 * Scheduler.isTraining if is true -> only capacity will be checked
		 */
		if (rejectionReason == ScheduleResponse.RejectionReason.none) {

			volume = maxAvailableCapacityBackEnd.createVolumeThenAdd(
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

			System.out.println("[Failed to schedule] ->"
					+ volumeRequest.toString());
		} else {

			schedulerResponse.isSuccessful = true;

			schedulerResponse.backEndScheduled = maxAvailableCapacityBackEnd;

			System.out.println("[Successfully scheduled] ->"
					+ volumeRequest.toString() + " backendID= "
					+ schedulerResponse.backEndScheduled.getID());
		}

		/*
		 * first save the schedule response then the volume
		 */
		schedulerResponse.save(rejectionReason);

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
