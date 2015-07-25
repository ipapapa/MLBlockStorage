package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;

public class StatisticalGroupping extends Scheduler {
	public StatisticalGroupping(edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);
	}

	private double PredictedSTD = 53; // 52.5468;

	private double PredictedTotalCapacity = 138000; // 138057;

	private double predicatedAVG = 41; // 41.4823;

	BackEndSpecifications backendSpecifications = new BackEndSpecifications(
			1200, 2000, 800, 500, 800, 200, 0, true);

	private void RankRequest(VolumeRequest request) {

		double STDDistanceFromMean = (request.getCapacity() - this.predicatedAVG)
				/ this.PredictedSTD;

		STDDistanceFromMean = Math.round(STDDistanceFromMean * 10)
				/ (double) 10;

		GroupSize size;

		if ((STDDistanceFromMean >= GroupSize.Small.lowerBound1 && STDDistanceFromMean <= GroupSize.Small.upperBound1)
				|| (STDDistanceFromMean >= GroupSize.Small.lowerBound2 && STDDistanceFromMean <= GroupSize.Small.upperBound2)) {
			// group small

			size = GroupSize.Small;

		} else if ((STDDistanceFromMean >= GroupSize.Medium.lowerBound1 && STDDistanceFromMean < GroupSize.Medium.upperBound1)
				|| (STDDistanceFromMean > GroupSize.Medium.lowerBound2 && STDDistanceFromMean <= GroupSize.Medium.upperBound2)) {
			// group medium

			size = GroupSize.Medium;

		} else if ((STDDistanceFromMean >= GroupSize.Large.lowerBound1 && STDDistanceFromMean < GroupSize.Large.upperBound1)
				|| (STDDistanceFromMean > GroupSize.Large.lowerBound2 && STDDistanceFromMean <= GroupSize.Large.upperBound2)) {
			// Group large

			size = GroupSize.Large;

		} else if ((STDDistanceFromMean >= GroupSize.XLarge.lowerBound1 && STDDistanceFromMean < GroupSize.XLarge.upperBound1)
				|| (STDDistanceFromMean > GroupSize.XLarge.lowerBound2 && STDDistanceFromMean <= GroupSize.XLarge.upperBound2)) {
			// Group X-large

			size = GroupSize.XLarge;
		} else {
			// Group XX-large

			size = GroupSize.XXLarge;
		}

		request.setGroupSize(size);
	}

	private void addNewBackends() throws SQLException {
		// 1200 is an assumption
		@SuppressWarnings("unused")
		int OptimalBinNumbers = (int) (this.PredictedTotalCapacity / 1200);

		super.getExperiment()
				.AddBackEnd(backendSpecifications, GroupSize.Small);

		super.getExperiment()
				.AddBackEnd(backendSpecifications, GroupSize.Small);

		super.getExperiment().AddBackEnd(backendSpecifications,
				GroupSize.Medium);

		super.getExperiment().AddBackEnd(backendSpecifications,
				GroupSize.Medium);

		super.getExperiment()
				.AddBackEnd(backendSpecifications, GroupSize.Large);

		super.getExperiment()
				.AddBackEnd(backendSpecifications, GroupSize.Large);

		super.getExperiment().AddBackEnd(backendSpecifications,
				GroupSize.XLarge);

		super.getExperiment().AddBackEnd(backendSpecifications,
				GroupSize.XXLarge);

		super.getExperiment().AddBackEnd(backendSpecifications,
				GroupSize.XXXLarge);
	}

	private static int start;

	private Backend GetBestBackEndForRequest(VolumeRequest request)
			throws SQLException {
		int MaxGroupSize = 6;

		start = request.getGroupSize().order;

		for (int i = 1; i <= MaxGroupSize; i++) {

			this.getExperiment();
			List<Backend> candidateList = edu.purdue.simulation.Experiment.BackEndList
					.stream()
					.filter(b -> (b.getGroupSize().order == start)
							&& (b.getState().getAvailableCapacity() > request
									.getCapacity()))
					.collect(Collectors.toList());

			if (candidateList.size() > 0) {
				return candidateList.get(0);
			}

			start = start + 1;

			if (start > MaxGroupSize)

				start = 1;

		}

		this.addNewBackends();

		return this.GetBestBackEndForRequest(request);
	}

	public void schedule() throws SQLException {

		VolumeRequest request = super.getRequestQueue().peek();

		this.RankRequest(request);

		ScheduleResponse schedulerResponse = new ScheduleResponse( //
				this.getExperiment(), //
				request);

		Backend bestFit = this.GetBestBackEndForRequest(request);

		Volume volume = bestFit.createVolumeThenSave(request.ToVolumeSpecifications(),
				schedulerResponse);

		BackEndSpecifications backendSpecifications = new BackEndSpecifications(
				1200, 2000, 800, 500, 800, 200, 0, true);

		// here volume will never be null
		if (volume == null) {

			schedulerResponse.isSuccessful = false;

			schedulerResponse.backEndScheduled = null;

			schedulerResponse.backEndCreated = super.getExperiment()
					.AddBackEnd(backendSpecifications); // no backend created

			// no backend turned on
			schedulerResponse.backEndTurnedOn = null;

			System.out.println("Failed to schedule ->" + request.toString());
		} else {

			schedulerResponse.isSuccessful = true;

			schedulerResponse.backEndScheduled = bestFit;

			System.out
					.println("Successfully scheduled ->" + request.toString());

			super.getRequestQueue().remove();
		}

		schedulerResponse.Save();
	}
}
