package edu.purdue.simulation;

import java.io.Console;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;

import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.Volume;
import edu.purdue.simulation.blockstorage.VolumeRequestCategories;
import edu.purdue.simulation.blockstorage.VolumeSpecifications;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.BackendCategories;
import edu.purdue.simulation.blockstorage.backend.LVM;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;

public class Experiment extends PersistentObject {

	public Experiment(Workload workload, String comment,
			String schedulerAlgorithm) {
		super();

		this.setComment(comment);

		this.setSchedulerAlgorithm(schedulerAlgorithm);

		Experiment.backEndList = new ArrayList<>();

		Experiment.clock = new BigDecimal(1);

		this.setWorkload(workload);
	}

	private String comment;

	private Workload workload;

	public String schedulerAlgorithm;

	public static ArrayList<Backend> backEndList;

	public static BigDecimal clock;

	public String getSchedulerAlgorithm() {
		return this.schedulerAlgorithm;
	}

	public void setSchedulerAlgorithm(String schedulerAlgorithm) {
		this.schedulerAlgorithm = schedulerAlgorithm;
	}

	public String getComment() {
		return (this.comment + " ").trim() //
				+ " ResourceMonitor.enableBackendPerformanceMeter = "
				+ ResourceMonitor.enableBackendPerformanceMeter //
				+ " \nResourceMonitor.enableVolumePerformanceMeter = "
				+ ResourceMonitor.enableVolumePerformanceMeter //
				+ " \nResourceMonitor.clockGap = "
				+ ResourceMonitor.clockGap //
				+ " \nScheduler.maxClock = "
				+ Scheduler.maxClock //
				+ " \nScheduler.schedulePausePoissonMean = "
				+ Scheduler.schedulePausePoissonMean //
				+ " \nScheduler.devideVolumeDeleteProbability = "
				+ Scheduler.devideVolumeDeleteProbability //
				+ " \nStochasticEventGenerator.clockGap = "
				+ StochasticEventGenerator.clockGap //
		;
	}

	public Workload getWorkload() {
		return workload;
	}

	public void setWorkload(Workload workload) {
		this.workload = workload;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public ArrayList<Backend> generateBackEnd() throws SQLException {

		if (this.getID() == null
				|| !(this.getID().compareTo(BigDecimal.ZERO) > 0))

			this.save();

		backEndList = new ArrayList<Backend>();

		BackEndSpecifications specification = new BackEndSpecifications(1200,
				2000, 800, 500, 800, 200, 0, true, 10000);

		BackEndSpecifications[] specifications = { specification,
				specification, specification, specification, specification,
				specification, specification, };

		for (int i = 0; i < specifications.length; i++) {
			Backend backEnd = new LVM(this, "NO DESCRIPTION", specifications[i]);

			backEnd.save();

			backEndList.add(backEnd);
		}

		return backEndList;

	}

	public static Experiment resumeExperiment(BigDecimal experimentID)
			throws SQLException {

		ResultSet experimentResultSet = Database.executeQuery(
				"Select	*	From	experiment	Where	ID	= ?", experimentID);

		Experiment experiment = null;

		if (experimentResultSet.next()) {

			ResultSet workloadResultSet = Database
					.executeQuery(
							"Select	ID, Generate_Method, Comment	From	Workload	Where	ID	= ?",
							experimentResultSet.getBigDecimal(2));

			Workload workload = null;

			if (workloadResultSet.next()) {
				workload = new Workload(//
						experimentResultSet.getInt(2), // generateMethod
						experimentResultSet.getString(3) // comment
				);

				workload.setID(workloadResultSet.getBigDecimal(1));
			}

			experiment = new Experiment(//
					workload,// workload
					experimentResultSet.getString(2),//
					experimentResultSet.getString(3));

			experiment.setID(experimentResultSet.getBigDecimal(1));

			ResultSet lastestVolumeRequestDataSet = Database
					.executeQuery(
							"Select	SR.volume_request_ID	From	schedule_response	SR	Where	SR.experiment_id	=	?	Order	By	SR.ID	Desc	Limit	1",
							experimentID);

			if (lastestVolumeRequestDataSet.next()) {
				workload.RetrieveVolumeRequests(lastestVolumeRequestDataSet
						.getBigDecimal(1));
			}
		}

		ResultSet rs = Database
				.executeQuery(
						"Select	*	From	Backend	Where	MaxCapacity Is Not Null and	experiment_id	= ?",
						experimentID);

		while (rs.next()) {

			BackEndSpecifications backendSpecifications = new BackEndSpecifications(
					rs.getInt(5),// Capacity
					rs.getInt(6),// MaxCapacity
					rs.getInt(7),// Min Capacity
					rs.getInt(8),// IOPS
					rs.getInt(9),// MaxIOPS
					rs.getInt(10),// MinIOPS
					0, // latency
					rs.getBoolean(11),// isOnline
					rs.getDouble(13));

			Backend backend = new LVM(//
					experiment,//
					rs.getString(12),// Description
					backendSpecifications);

			backend.setID(rs.getBigDecimal(1));

			Experiment.backEndList.add(backend);
		}

		for (int i = 0; i < Experiment.backEndList.size(); i++) {

			Backend backend = Experiment.backEndList.get(i);

			ResultSet volumesResultSet = Database
					.executeQuery(
							"Select	*	From	volume	where	is_deleted = 0 and	backend_ID	= ?	order by	ID	Desc",
							backend.getID());

			while (volumesResultSet.next()) {

				VolumeSpecifications volumeSpecifications = new VolumeSpecifications(
						volumesResultSet.getInt(4), // capacity
						volumesResultSet.getInt(5), // IOPS
						0, // latency
						volumesResultSet.getBoolean(6), // isDeleted
						volumesResultSet.getDouble(7));

				Volume volume = new Volume(backend, null, volumeSpecifications);

				volume.setID(volumesResultSet.getBigDecimal(1));

				backend.getVolumeList().add(volume);
			}
		}

		ResultSet clockResultSet = Database
				.executeQuery(
						"select	Clock	From	volume_performance_meter	Where	experiment_id		= ?	Order	By	ID	Desc	Limit	1",
						experimentID);

		if (clockResultSet.next()) {
			Experiment.clock = clockResultSet.getBigDecimal(1);
		}

		return experiment;

	}

	public Backend addBackEnd(String description,
			BackEndSpecifications backEndSpecifications,
			VolumeRequestCategories groupSize) throws SQLException {

		Backend backEnd = this.addBackEnd(description, backEndSpecifications);

		// I cant understand why is needed in BackEndSpecifications for
		// statisticalGroupping method
		// backEnd.setGroupSize(groupSize);

		return backEnd;
	}

	public Backend addBackEnd(String description,
			BackEndSpecifications backEndSpecifications) throws SQLException {

		Backend backEnd = new LVM(this, description, backEndSpecifications);

		backEnd.save();

		backEndList.add(backEnd);

		return backEnd;
	}

	public void update() throws SQLException {
		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"Update	experiment	Set	scheduler_algorithm	=?, comment= ?, workload_ID = ?	Where	ID	= ?",
						Statement.RETURN_GENERATED_KEYS);

		statement.setString(1, this.getSchedulerAlgorithm());

		statement.setString(2, this.getComment());

		statement.setBigDecimal(3, this.getWorkload().getID());

		statement.setBigDecimal(4, this.getID());

		statement.executeUpdate();
	}

	@Override
	public BigDecimal save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(
				"insert into experiment "
						+ "	(comment, scheduler_algorithm, workload_ID)"
						+ "		Values" + "	(?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS);

		statement.setString(1, this.getComment());

		statement.setString(2, this.getSchedulerAlgorithm());

		statement.setBigDecimal(3, this.getWorkload().getID());

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.setID(rs.getBigDecimal(1));

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public String toString() {
		return String.format("ID: %d - SchedulerAlgorithm: %d - comment: %d",
				this.getID().toString(), this.getSchedulerAlgorithm(),
				this.getComment());
	}
}
