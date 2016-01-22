package edu.purdue.simulation.blockstorage.backend;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.PersistentObject;
import edu.purdue.simulation.blockstorage.Volume;

public class BackendPerformanceMeter extends PersistentObject {

	public BackendPerformanceMeter(Backend backEnd) {
		this.backend = backEnd;
	}

	private Backend backend;

	// public BigDecimal clock;// not thread safe

	public int availableCapacity;

	public String toString() {
		return "[BackendPerformanceMeter] clock = " + Experiment.clock
				+ " ID = " + this.getID() + " backendID = "
				+ this.backend.getID();
	}

	public BigDecimal Save(Volume pingVolume) throws SQLException, Exception {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"Insert Into BlockStorageSimulator.backend_performance_meter"
								+ "	(backend_ID, volume_ID, clock, available_IOPS, available_capacity, volumes_count)"
								+ "		values" + "	(?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.backend.getID());

		statement.setBigDecimal(2, pingVolume.getID());

		statement.setBigDecimal(3, Experiment.clock);

		int currentIOPS = pingVolume.getAvailableIOPS_ForEachVolume();

		statement.setInt(4, currentIOPS);

		int availableCapacity = backend.getState().getAvailableCapacity();

		statement.setInt(5, availableCapacity);

		statement.setInt(6, this.backend.getVolumeList().size());

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.setID(rs.getBigDecimal(1));

			edu.purdue.simulation.BlockStorageSimulator.log(this.toString() + " pingVolumeID = "
					+ pingVolume.getID() + " currentIOPS = " + currentIOPS
					+ " availableCapacity = " + availableCapacity);

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}
}
