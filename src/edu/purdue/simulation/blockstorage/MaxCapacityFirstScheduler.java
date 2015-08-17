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
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;

public class MaxCapacityFirstScheduler extends Scheduler {

	private weka.classifiers.trees.REPTree repTree;

	public MaxCapacityFirstScheduler(
			edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);

		String arguments = "-t D:\\SAS\\2\\514Cat_g3.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c 3";

		this.repTree = new REPTree();

		AbstractClassifier.runClassifier(this.repTree, arguments.split(" "));
	}

	@SuppressWarnings("unused")
	protected void preRun() throws SQLException {

		super.getExperiment().addBackEnd("Back 1", new BackEndSpecifications(//
				300, // Capacity
				500,// Max Capacity
				350, // Min capacity
				600, // IOPS
				800, // Max IOPS
				400, // Min IOPS
				0, // Latency
				true, // is online
				100 // stabilityPossessionMean
				));

		super.getExperiment().addBackEnd("Back 2", new BackEndSpecifications(//
				300, // Capacity
				500,// Max Capacity
				350, // Min capacity
				600, // IOPS
				800, // Max IOPS
				400, // Min IOPS
				0, // Latency
				true, // is online
				100 // stabilityPossessionMean
				));
		
		super.getExperiment().addBackEnd("Back 3", new BackEndSpecifications(//
				300, // Capacity
				500,// Max Capacity
				350, // Min capacity
				600, // IOPS
				800, // Max IOPS
				400, // Min IOPS
				0, // Latency
				true, // is online
				100 // stabilityPossessionMean
				));
		
		super.getExperiment().addBackEnd("Back 4", new BackEndSpecifications(//
				300, // Capacity
				500,// Max Capacity
				350, // Min capacity
				600, // IOPS
				800, // Max IOPS
				400, // Min IOPS
				0, // Latency
				true, // is online
				100 // stabilityPossessionMean
				));
		
		super.getExperiment().addBackEnd("Back 4", new BackEndSpecifications(//
				300, // Capacity
				500,// Max Capacity
				350, // Min capacity
				600, // IOPS
				800, // Max IOPS
				400, // Min IOPS
				0, // Latency
				true, // is online
				100 // stabilityPossessionMean
				));
		
		if (false) {

			super.getExperiment().addBackEnd("", new BackEndSpecifications(//
					300, // Capacity
					250,// Max Capacity
					150, // Min capacity
					400, // IOPS
					800, // Max IOPS
					100, // Min IOPS
					0, // Latency
					true, // is online
					50 // stabilityPossessionMean
					));

			super.getExperiment().addBackEnd("", new BackEndSpecifications(//
					300, // Capacity
					800,// Max Capacity
					350, // Min capacity
					800, // IOPS
					1500, // Max IOPS
					550, // Min IOPS
					0, // Latency
					true, // is online
					200 // stabilityPossessionMean
					));

			super.getExperiment().addBackEnd("", new BackEndSpecifications(//
					300, // Capacity
					850,// Max Capacity
					320, // Min capacity
					760, // IOPS
					950, // Max IOPS
					520, // Min IOPS
					0, // Latency
					true, // is online
					180 // stabilityPossessionMean
					));
		}

	}

	private boolean doScheduler(Backend backend,
			VolumeSpecifications volumeSpecifications) {

		Instance instance = new DenseInstance(4);

		int clock = Experiment.clock.intValue();

		int backendSize = backend.getVolumeList().size();

		int totalRequestedCap = 0;

		for (int i = 0; i < backendSize; i++) {
			totalRequestedCap += backend.getVolumeList().get(i)
					.getSpecifications().getIOPS();
		}

		//instance.setValue(0, (clock % 48) / 2);
		instance.setValue(0, clock % 24);
		instance.setValue(1, backendSize + 1);
		// instance.setValue(3, 10);
		instance.setValue(3, totalRequestedCap + volumeSpecifications.getIOPS());

		try {
			double[] predictors = this.repTree
					.distributionForInstance(instance);

			if (predictors[0] > predictors[1] + predictors[2] + predictors[3])

				return true;

			else

				return false;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return false;

	}

	public void schedule() throws SQLException {

		Collections.sort(edu.purdue.simulation.Experiment.backEndList,
				new Comparator<Backend>() {
					@Override
					public int compare(Backend backEnd1, Backend backEnd2) {
						return Integer.compare(backEnd2.getState()
								.getAvailableCapacity(), backEnd1.getState()
								.getAvailableCapacity());
					}
				});

		Backend maxAvailableCapacityBackEnd = edu.purdue.simulation.Experiment.backEndList
				.get(0);

		VolumeRequest request = super.getRequestQueue().peek();

		ScheduleResponse schedulerResponse = new ScheduleResponse( //
				this.getExperiment(), //
				request);

		if (Experiment.clock.intValue() > 495) {
			// maxAvailableCapacityBackEnd.createRegressionModel();
			int ii = 1;
		}

		VolumeSpecifications requestedSpecifications = request
				.ToVolumeSpecifications();

		Volume volume = null;

		int requestedIOPS = requestedSpecifications.getIOPS();

		// TODO check the available IOPS, print injecetion reason\

		int avail = maxAvailableCapacityBackEnd
				.getAvailableIOPSWithRegression();

		boolean MLResult = doScheduler(maxAvailableCapacityBackEnd,
				requestedSpecifications);

		// if (!Scheduler.considerIOPS || avail > requestedIOPS) {
		// volume = maxAvailableCapacityBackEnd.createVolumeThenAdd(
		// requestedSpecifications, schedulerResponse);
		// }

		if (MLResult)

			volume = maxAvailableCapacityBackEnd.createVolumeThenAdd(
					requestedSpecifications, schedulerResponse);

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

		schedulerResponse.save(); // first save this then volume

		if (volume != null)

			volume.save();
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "MaxCapacityFirstScheduler";
	}

}
