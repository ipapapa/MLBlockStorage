package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Workload;

public class VolumeRequest extends Specifications {

	public VolumeRequest(Workload workload) {

		super();

		this.setWorkload(workload);

	}

	public VolumeRequest(Workload workload, int type, int capacity, int IOPS) {
		super();

		this.setWorkload(workload);

		this.setCapacity(capacity);

		this.setIOPS(IOPS);

		this.setType(type);
	}

	private GroupSize GroupSize;

	private int Type;

	private Workload Workload;

	public GroupSize getGroupSize() {
		return GroupSize;
	}

	public void setGroupSize(GroupSize groupSize) {
		GroupSize = groupSize;
	}

	public int getType() {
		return Type;
	}

	public void setType(int type) {
		Type = type;
	}

	public Workload getWorkload() {
		return Workload;
	}

	public void setWorkload(Workload workload) {
		Workload = workload;
	}

	public VolumeSpecifications ToVolumeSpecifications() {
		VolumeSpecifications result = new VolumeSpecifications(
				this.getCapacity(), this.getIOPS(), this.getLatency(), false);

		return result;
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(
				"insert into volume_request"
						+ "	(workload_id, capacity, type, IOPS)" + "		Values"
						+ "	(?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.Workload.getID());

		statement.setInt(2, this.getCapacity());

		statement.setInt(3, this.getType());

		statement.setInt(4, this.getIOPS());

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.setID(rs.getBigDecimal(1));

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public boolean RetrieveProperties(ResultSet resulSet) throws SQLException {

		if (this.getWorkload() == null) {
			// retrieve workload from DB but IT IS NOT LOGICAL

			throw new SQLException("workload must not be null");
		}

		super.setCapacity(resulSet.getInt(3));

		this.setType(resulSet.getInt(4));

		super.setIOPS(resulSet.getInt(5));

		super.RetrievePersistentProperties(resulSet, 6);

		return true;
	}

	@Override
	public String toString() {
		return String.format("%s ID: %s - Type: %d", //
				super.toString(), //
				this.getID().toString(), //
				this.Type); //
	}
}
