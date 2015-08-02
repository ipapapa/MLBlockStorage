package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;

import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;

public class MaxCapacityFirstScheduler extends Scheduler {

	public MaxCapacityFirstScheduler(
			edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);

	}

	protected void preRun() throws SQLException {

		BackEndSpecifications stableSpecifications = new BackEndSpecifications(
				400, 500, 350, 600, 800, 400, 0, true);

		BackEndSpecifications unstableSpecifications = new BackEndSpecifications(
				400, 500, 350, 600, 1200, 250, 0, true);

		super.getExperiment().AddBackEnd("", stableSpecifications);

		super.getExperiment().AddBackEnd("", stableSpecifications);

		super.getExperiment().AddBackEnd("", unstableSpecifications);

		super.getExperiment().AddBackEnd("", unstableSpecifications);

		// super.getExperiment().AddBackEnd(this.specifications);
		//
		// super.getExperiment().AddBackEnd(this.specifications);
		//
		// super.getExperiment().AddBackEnd(this.specifications);
		//
		// super.getExperiment().AddBackEnd(this.specifications);
		//
		// super.getExperiment().AddBackEnd(this.specifications);

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

		// TODO check the available IOPS, print injecetion reason\

		Volume volume = maxAvailableCapacityBackEnd.createVolumeThenSave(
				request.ToVolumeSpecifications(), schedulerResponse);

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

		schedulerResponse.Save();
	}

}
