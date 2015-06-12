package edu.purdue.simulation.blockstorage.backend;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.*;

public abstract class BackEnd {

	public BackEnd(Experiment experiment, BackEndSpecifications specifications) {
		this.VolumeList = new ArrayList<Volume>();

		this.Specifications = specifications;

		this.Specifications.setBackEnd(this);

		this.State = new State(specifications.getCapacity());

		this.Experiment = experiment;

		// 600, 800, 1000, 1600
	}

	private GroupSize GroupSize;

	public Experiment Experiment;

	private State State;

	public BackEndSpecifications Specifications;

	private List<Volume> VolumeList;

	public BigDecimal ID;

	public GroupSize getGroupSize() {
		return GroupSize;
	}

	public void setGroupSize(GroupSize groupSize) {
		GroupSize = groupSize;
	}

	public List<Volume> getVolumeList() {
		return VolumeList;
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(
				"insert into backend"
						+ "	(experiment_id, capacity, IOPS, is_online)"
						+ "		Values" + "	(?, ?, ?, ?);",
				Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.Experiment.ID);

		statement.setInt(2, this.Specifications.getCapacity());

		statement.setInt(3, this.Specifications.getIOPS());

		statement.setBoolean(4, this.Specifications.IsOnline);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.ID = rs.getBigDecimal(1);

			return this.ID;
		}

		return BigDecimal.valueOf(-1);
	}

	public State getState() {

		int usedSpace = 0;

		for (Iterator<Volume> i = this.getVolumeList().iterator(); i.hasNext();) {

			usedSpace += i.next().getSpecifications().getCapacity();

		}

		this.State.UsedSpace = usedSpace;

		return this.State;
	}

	public Volume CreateVolume(VolumeSpecifications volumeSpecifications,
			ScheduleResponse scheduleRequest) {

		if (this.getState().getAvailableCapacity() < volumeSpecifications
				.getCapacity()) {
			return null;
		}

		Volume result = new Volume(this, scheduleRequest, volumeSpecifications);

		this.VolumeList.add(result);

		return result;
	}

	@Override
	public String toString() {
		return String
				.format("ID: %s - Capacity: %d - IOPS: %d - IsOnline: %b - Latency: %d",
						this.ID.toString(), this.Specifications.getCapacity(),
						this.Specifications.getIOPS(),
						this.Specifications.IsOnline,
						this.Specifications.getLatency());
	}

}
