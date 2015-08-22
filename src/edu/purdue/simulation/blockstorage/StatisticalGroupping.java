package edu.purdue.simulation.blockstorage;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;

public class StatisticalGroupping extends Scheduler {
	public StatisticalGroupping(edu.purdue.simulation.Experiment experiment,
			edu.purdue.simulation.Workload workload) {
		super(experiment, workload);
	}

	private double PredictedSTD = 53; // 52.5468;

	private double PredictedTotalCapacity = 138000; // 138057;

	private double predicatedAVG = 41; // 41.4823;

	BackEndSpecifications backendSpecifications = new BackEndSpecifications(
			1200, 2000, 800, 500, 800, 200, 0, true, 50, null, null);

	private void RankRequest(VolumeRequest request) {

		double STDDistanceFromMean = (request.getCapacity() - this.predicatedAVG)
				/ this.PredictedSTD;

		STDDistanceFromMean = Math.round(STDDistanceFromMean * 10)
				/ (double) 10;

		VolumeRequestCategories size;

		if ((STDDistanceFromMean >= VolumeRequestCategories.Small.lowerBound1 && STDDistanceFromMean <= VolumeRequestCategories.Small.upperBound1)
				|| (STDDistanceFromMean >= VolumeRequestCategories.Small.lowerBound2 && STDDistanceFromMean <= VolumeRequestCategories.Small.upperBound2)) {
			// group small

			size = VolumeRequestCategories.Small;

		} else if ((STDDistanceFromMean >= VolumeRequestCategories.Medium.lowerBound1 && STDDistanceFromMean < VolumeRequestCategories.Medium.upperBound1)
				|| (STDDistanceFromMean > VolumeRequestCategories.Medium.lowerBound2 && STDDistanceFromMean <= VolumeRequestCategories.Medium.upperBound2)) {
			// group medium

			size = VolumeRequestCategories.Medium;

		} else if ((STDDistanceFromMean >= VolumeRequestCategories.Large.lowerBound1 && STDDistanceFromMean < VolumeRequestCategories.Large.upperBound1)
				|| (STDDistanceFromMean > VolumeRequestCategories.Large.lowerBound2 && STDDistanceFromMean <= VolumeRequestCategories.Large.upperBound2)) {
			// Group large

			size = VolumeRequestCategories.Large;

		} else if ((STDDistanceFromMean >= VolumeRequestCategories.XLarge.lowerBound1 && STDDistanceFromMean < VolumeRequestCategories.XLarge.upperBound1)
				|| (STDDistanceFromMean > VolumeRequestCategories.XLarge.lowerBound2 && STDDistanceFromMean <= VolumeRequestCategories.XLarge.upperBound2)) {
			// Group X-large

			size = VolumeRequestCategories.XLarge;
		} else {
			// Group XX-large

			size = VolumeRequestCategories.XXLarge;
		}

		request.setGroupSize(size);
	}

	private void addNewBackends() throws SQLException {
		// 1200 is an assumption
		@SuppressWarnings("unused")
		int OptimalBinNumbers = (int) (this.PredictedTotalCapacity / 1200);

		String description = "StatisticalGroupping NO DESCC";

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.Small);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.Small);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.Medium);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.Medium);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.Large);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.Large);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.XLarge);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.XXLarge);

		super.getExperiment().addBackEnd(description, backendSpecifications,
				VolumeRequestCategories.XXXLarge);
	}

	private static int start;

	private Backend GetBestBackEndForRequest(VolumeRequest request)
			throws SQLException {
		int MaxGroupSize = 6;

		start = request.getGroupSize().order;

		for (int i = 1; i <= MaxGroupSize; i++) {

			this.getExperiment();
			List<Backend> candidateList = edu.purdue.simulation.Experiment.backendList
					.stream()
					// I CANT UNDERSTAND WHY getGroupSize is needed
					// .filter(b -> (b.getGroupSize().order == start)
					// && (b.getState().getAvailableCapacity() > request
					// .getCapacity()))
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

		Volume volume = bestFit.createVolumeThenAdd(
				request.ToVolumeSpecifications(), schedulerResponse);

		BackEndSpecifications backendSpecifications = new BackEndSpecifications(
				1200, 2000, 800, 500, 800, 200, 0, true, 50, null, null);

		// here volume will never be null
		if (volume == null) {

			schedulerResponse.isSuccessful = false;

			schedulerResponse.backEndScheduled = null;

			schedulerResponse.backEndCreated = super.getExperiment()
					.addBackEnd("StatisticalGroupping NO DES",
							backendSpecifications); // no backend created

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

		if (volume == null) {

			schedulerResponse.save(ScheduleResponse.RejectionReason.Capacity);

		} else {
			schedulerResponse.save(null);

			volume.save();
		}
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "StatisticalGroupping";
	}
}
