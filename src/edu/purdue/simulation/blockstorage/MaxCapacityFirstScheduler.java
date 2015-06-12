package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;

import edu.purdue.simulation.blockstorage.backend.BackEnd;

public class MaxCapacityFirstScheduler extends Scheduler {

	public MaxCapacityFirstScheduler(
			edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);
		// TODO Auto-generated constructor stub
	}

	public void Schedule() throws SQLException {

		for (int i = 0; i < 100; i++)
			// 119800 / 1200 = 100 optimum number of requests

			super.getExperiment().AddBackEnd(1200);

		while (!this.getRequestQueue().isEmpty()) {

			Collections.sort(this.getExperiment().BackEndList,
					new Comparator<BackEnd>() {
						@Override
						public int compare(BackEnd backEnd1, BackEnd backEnd2) {
							return Integer.compare(backEnd2.getState()
									.getAvailableCapacity(), backEnd1
									.getState().getAvailableCapacity());
						}
					});

			BackEnd maxAvailableCapacityBackEnd = this.getExperiment().BackEndList
					.get(0);

			VolumeRequest request = super.getRequestQueue().peek();

			ScheduleResponse schedulerResponse = new ScheduleResponse( //
					this.getExperiment(), //
					request);

			Volume volume = maxAvailableCapacityBackEnd.CreateVolume(
					request.ToVolumeSpecifications(), schedulerResponse);

			// there is no volume available. either reject or create new volume
			// in case of reject a SchedulerResponse record will be saved
			if (volume == null) {

				schedulerResponse.IsSuccessful = false;

				schedulerResponse.BackEndScheduled = null;

				schedulerResponse.BackEndCreated = super.getExperiment()
						.AddBackEnd(1200); // no backend created

				schedulerResponse.BackEndTurnedOn = null; // no backend turned
															// on

				System.out
						.println("Failed to schedule ->" + request.toString());
			} else {

				schedulerResponse.IsSuccessful = true;

				schedulerResponse.BackEndScheduled = maxAvailableCapacityBackEnd;

				System.out.println("Successfully scheduled ->"
						+ request.toString());

				super.getRequestQueue().remove();
			}

			schedulerResponse.Save();

			if (volume != null)

				volume.Save();
		}
	}

}
