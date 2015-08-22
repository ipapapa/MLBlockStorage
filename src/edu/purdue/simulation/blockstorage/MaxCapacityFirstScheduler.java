package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;

import weka.classifiers.AbstractClassifier;
import weka.classifiers.trees.REPTree;
import weka.core.DenseInstance;
import weka.core.Instance;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.blockstorage.ScheduleResponse.RejectionReason;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;

public class MaxCapacityFirstScheduler extends Scheduler {

	public MaxCapacityFirstScheduler(
			edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);
	}

	@SuppressWarnings("unused")
	protected void preRun() throws SQLException {

		int capacity = 7200;

		int bandwidth = 1948;

		int maxBandwidth = bandwidth + 500;

		int minBandwidth = bandwidth - 500;

		String path = "D:\\Research\\experiment\\Custom\\";

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
				path + "backend1_186752_ex594.arff",//
				MachineLearningAlgorithm.RepTree));

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
				path + "backend2_186753_ex594.arff",//
				MachineLearningAlgorithm.RepTree));

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
				path + "backend3_186754_ex594.arff",//
				MachineLearningAlgorithm.RepTree));

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
				path + "backend4_186755_ex594.arff",//
				MachineLearningAlgorithm.RepTree));

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
				path + "backend5_186756_ex594.arff",//
				MachineLearningAlgorithm.RepTree));

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
				path + "backend6_186757_ex594.arff", //
				MachineLearningAlgorithm.RepTree));
	}

	public void schedule() throws java.lang.Exception {

		super.sortBackendListBaseOnAvailableCapacity();

		Backend maxAvailableCapacityBackEnd = edu.purdue.simulation.Experiment.backendList
				.get(0);

		VolumeRequest request = super.getRequestQueue().peek();

		ScheduleResponse schedulerResponse = new ScheduleResponse( //
				this.getExperiment(), //
				request);

		// if (Experiment.clock.intValue() > 495) {
		// // maxAvailableCapacityBackEnd.createRegressionModel();
		// int ii = 1;
		// }

		VolumeSpecifications requestedSpecifications = request
				.ToVolumeSpecifications();

		Volume volume = null;

		// int requestedIOPS = requestedSpecifications.getIOPS();

		ScheduleResponse.RejectionReason rejectionReason = ScheduleResponse.RejectionReason.none;

		if (Scheduler.isTraining == false)

			rejectionReason = super.validateResources(
					maxAvailableCapacityBackEnd, requestedSpecifications,
					MachineLearningAlgorithm.RepTree);

		// Scheduler.isTraining if is true -> only capacity will be checked
		if (Scheduler.isTraining
				|| rejectionReason == ScheduleResponse.RejectionReason.none) {

			volume = maxAvailableCapacityBackEnd.createVolumeThenAdd(
					requestedSpecifications, schedulerResponse);
		}

		// there is no volume available. either reject or create new volume
		// in case of reject a SchedulerResponse record will be saved
		if (volume == null) {

			schedulerResponse.isSuccessful = false;

			schedulerResponse.backEndScheduled = null;

			// I only want to have 1 backend
			// schedulerResponse.backEndCreated = super.getExperiment()
			// .AddBackEnd(this.specifications); // no backend created

			// if cant schedule, drop it
			super.getRequestQueue().remove();

			schedulerResponse.backEndTurnedOn = null; // no backend turned on

			System.out.println("[Failed to schedule] ->" + request.toString());
		} else {

			schedulerResponse.isSuccessful = true;

			schedulerResponse.backEndScheduled = maxAvailableCapacityBackEnd;

			System.out.println("[Successfully scheduled] ->"
					+ request.toString() + " backendID= "
					+ schedulerResponse.backEndScheduled.getID());

			super.getRequestQueue().remove();
		}

		schedulerResponse.save(rejectionReason); // first save this then volume

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
