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
import edu.purdue.simulation.PersistentObject;
import edu.purdue.simulation.blockstorage.*;

public abstract class Backend extends PersistentObject {

	public Backend(Experiment experiment, BackEndSpecifications specifications) {
		this.VolumeList = new ArrayList<Volume>();

		this.specifications = specifications;

		this.state = new State(this);

		this.Experiment = experiment;
	}

	private GroupSize GroupSize;

	private Experiment Experiment;

	private State state;

	private BackEndSpecifications specifications;

	private List<Volume> VolumeList;

	public Experiment getExperiment() {
		return Experiment;
	}

	public BackEndSpecifications getSpecifications() {
		return specifications;
	}

	public GroupSize getGroupSize() {
		return GroupSize;
	}

	public void setGroupSize(GroupSize groupSize) {
		GroupSize = groupSize;
	}

	public List<Volume> getVolumeList() {
		return VolumeList;
	}

	private BigDecimal doSave(boolean isUpdate, int operationID)
			throws SQLException {
		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert into backend"
								+ "	(experiment_id, capacity, IOPS, is_online, clock, MaxCapacity, MinCapacity, MaxIOPS, MinIOPS, operation_ID)"
								+ "		Values"
								+ "	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.Experiment.getID());

		statement.setInt(2, this.specifications.getCapacity());

		statement.setInt(3, this.specifications.getIOPS());

		statement.setBoolean(4, this.specifications.IsOnline);

		statement.setBigDecimal(5, edu.purdue.simulation.Experiment.clock);

		if (isUpdate) {

			statement.setNull(6, java.sql.Types.INTEGER);

			statement.setNull(7, java.sql.Types.INTEGER);

			statement.setNull(8, java.sql.Types.INTEGER);

			statement.setNull(9, java.sql.Types.INTEGER);

		} else {
			statement.setInt(6, this.specifications.getMaxCapacity());

			statement.setInt(7, this.specifications.getMinCapacity());

			statement.setInt(8, this.specifications.getMaxIOPS());

			statement.setInt(9, this.specifications.getMinIOPS());
		}

		statement.setInt(10, operationID);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			if (isUpdate == false)
				// Don't screw current IF
				this.setID(rs.getBigDecimal(1));

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}

	public BigDecimal save() throws SQLException {

		return this.doSave(false, 1); // 1 is create
	}

	public BigDecimal saveCurrentState() throws SQLException {

		return this.doSave(true, 2); // 2 save current state
	}

	public State getState() {

		return this.state;
	}

	public boolean removeVolume(Volume volume) {

		this.VolumeList.remove(volume);

		return true;
	}

	public Volume createVolumeThenSave(
			VolumeSpecifications volumeSpecifications,
			ScheduleResponse scheduleRequest, boolean isPingVolume)
			throws SQLException {

		if (isPingVolume) {

			// TODO actually implement delete volume which update the field
			// is_deleted
			volumeSpecifications = new VolumeSpecifications(0, 0, 0, true);

			scheduleRequest = null;

		} else if (this.getState().getAvailableCapacity() < volumeSpecifications
				.getCapacity()) {
			return null;
		}

		Volume result = new Volume(this, scheduleRequest, volumeSpecifications);

		result.Save();

		this.VolumeList.add(result);

		return result;
	}

	public Volume createPingVolumeThenSave() throws SQLException {

		return this.createVolumeThenSave(null, null, true);
	}

	public Volume createVolumeThenSave(
			VolumeSpecifications volumeSpecifications,
			ScheduleResponse scheduleRequest) throws SQLException {

		return this.createVolumeThenSave(volumeSpecifications, scheduleRequest,
				false);
	}

	@Override
	public String toString() {
		return String
				.format("ID: %s - Capacity: %d - IOPS: %d - IsOnline: %b - Latency: %d",
						this.getID().toString(),
						this.specifications.getCapacity(),
						this.specifications.getIOPS(),
						this.specifications.IsOnline,
						this.specifications.getLatency());
	}

}
