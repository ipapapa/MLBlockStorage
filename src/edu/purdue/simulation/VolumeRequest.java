package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.blockstorage.VolumeRequestCategories;
import edu.purdue.simulation.blockstorage.Specifications;
import edu.purdue.simulation.blockstorage.VolumeSpecifications;

public class VolumeRequest extends Specifications {

	public VolumeRequest(Workload workload) {

		super();

		this.setWorkload(workload);

	}

	public VolumeRequest(Workload workload, int type, int capacity, int IOPS,
			double deleteFactor, int arrivalTime) {
		super();

		this.setWorkload(workload);

		this.setCapacity(capacity);

		this.setIOPS(IOPS);

		this.setType(type);

		this.setDeleteFactor(deleteFactor);

		this.setArrivalTime(arrivalTime);
	}

	private VolumeRequestCategories GroupSize;

	private int Type;

	private Workload Workload;

	private double deleteFactor;

	private int arrivalTime;

	public int getArrivalTime() {
		return arrivalTime;
	}

	public void setArrivalTime(int arrivalTime) {
		this.arrivalTime = arrivalTime;
	}

	public double getDeleteFactor() {
		return deleteFactor;
	}

	public void setDeleteFactor(double deleteProbability) {
		this.deleteFactor = deleteProbability;
	}

	public VolumeRequestCategories getGroupSize() {
		return GroupSize;
	}

	public void setGroupSize(VolumeRequestCategories groupSize) {
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
				this.getCapacity(), //
				this.getIOPS(), //
				this.getLatency(), //
				false, //
				this.getDeleteFactor(), //
				Experiment.clock.intValue());

		return result;
	}

	public String getSaveQuery() {
		return String
				.format("insert into volume_request\n"
						+ "(workload_id,capacity,type,IOPS,Delete_Probability,Arrival_Time)\n"
						+ "Values(%d, %d, %d, %d, %f, %d)\n",//
						this.Workload.getID().intValue(),//
						this.getCapacity(), //
						this.getType(), //
						this.getIOPS(), //
						this.getDeleteFactor(), //
						this.getArrivalTime());
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert into volume_request"
								+ "	(workload_id, capacity, type, IOPS, Delete_Probability, Arrival_Time)"
								+ "		Values" + "	(?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.Workload.getID());

		statement.setInt(2, this.getCapacity());

		statement.setInt(3, this.getType());

		statement.setInt(4, this.getIOPS());

		statement.setDouble(5, this.getDeleteFactor());

		statement.setInt(6, this.getArrivalTime());

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.setID(rs.getBigDecimal(1));

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public boolean retrieveProperties(ResultSet resulSet) throws SQLException {

		if (this.getWorkload() == null) {
			// retrieve workload from DB but IT IS NOT LOGICAL

			throw new SQLException("workload must not be null");
		}
		// resulSet.getInt(1)
		super.setCapacity(resulSet.getInt(3));

		this.setType(resulSet.getInt(4));

		double requestedIOPS = resulSet.getInt(5);

		super.setIOPS((int) requestedIOPS); // lower the IOPS

		this.setDeleteFactor(resulSet.getDouble(6));

		this.setArrivalTime(resulSet.getInt(7));

		super.retrievePersistentProperties(resulSet, 8);

		return true;
	}

	@Override
	public String toString() {
		return String.format("Clock = %s - %s ID: %s - Type: %d", //
				Experiment.clock.toString(),//
				super.toString(), //
				this.getID(), //
				this.Type); 
	}
}
