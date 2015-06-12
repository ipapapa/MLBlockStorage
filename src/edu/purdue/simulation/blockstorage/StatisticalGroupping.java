package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import edu.purdue.simulation.blockstorage.backend.BackEnd;

public class StatisticalGroupping extends Scheduler {
	public StatisticalGroupping(edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);
		// TODO Auto-generated constructor stub
	}

	private double PredictedSTD = 53; // 52.5468;

	private double PredictedTotalCapacity = 138000; // 138057;

	private double predicatedAVG = 41; // 41.4823;

	private static final int BinSize = 1200; // GB

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
		int OptimalBinNumbers = (int) (this.PredictedTotalCapacity / 1200);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.Small);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.Small);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.Medium);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.Medium);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.Large);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.Large);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.XLarge);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.XXLarge);

		super.getExperiment().AddBackEnd(BinSize, GroupSize.XXXLarge);
	}

	private static int start;

	private BackEnd GetBestBackEndForRequest(VolumeRequest request)
			throws SQLException {
		int MaxGroupSize = 6;

		start = request.getGroupSize().order;

		for (int i = 1; i <= MaxGroupSize; i++) {

			List<BackEnd> candidateList = this.getExperiment().BackEndList
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

		// .sorted((a, b) -> Integer.compare(b.getState()
		// .getAvailableCapacity(), a.getState()
		// .getAvailableCapacity()));

		return this.GetBestBackEndForRequest(request);
	}

	public void Schedule() throws SQLException {

		while (!this.getRequestQueue().isEmpty()) {

			// Collections.sort(this.getExperiment().BackEndList,
			// new Comparator<BackEnd>() {
			// @Override
			// public int compare(BackEnd backEnd1, BackEnd backEnd2) {
			// return Integer.compare(backEnd2.getState()
			// .getAvailableCapacity(), backEnd1
			// .getState().getAvailableCapacity());
			// }
			// });
			//
			// BackEnd maxAvailableCapacityBackEnd =
			// this.getExperiment().BackEndList
			// .get(0);

			VolumeRequest request = super.getRequestQueue().peek();

			this.RankRequest(request);

			ScheduleResponse schedulerResponse = new ScheduleResponse( //
					this.getExperiment(), //
					request);

			BackEnd bestFit = this.GetBestBackEndForRequest(request);

			Volume volume = bestFit.CreateVolume(
					request.ToVolumeSpecifications(), schedulerResponse);

			// here volume will never be null
			if (volume == null) {

				schedulerResponse.IsSuccessful = false;

				schedulerResponse.BackEndScheduled = null;

				schedulerResponse.BackEndCreated = super.getExperiment()
						.AddBackEnd(1200); // no backend created

				// no backend turned on
				schedulerResponse.BackEndTurnedOn = null;

				System.out
						.println("Failed to schedule ->" + request.toString());
			} else {

				schedulerResponse.IsSuccessful = true;

				schedulerResponse.BackEndScheduled = bestFit;

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
