package edu.purdue.simulation.blockstorage;

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

public class VolumePerformanceMeter extends PersistentObject {

	public VolumePerformanceMeter(Volume volume) {

		this.volume = volume;

	}

	private Volume volume;

	public String toString() {
		return "[VOLUME PERFORMANCE] clock = " + Experiment.clock + " ID = "
				+ this.getID() + " volume_ID = " + this.volume.getID()
				+ " backendID = " + this.volume.getBackend().getID();
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(
				"Insert Into BlockStorageSimulator.volume_performance_meter"
						+ "	(experiment_ID, volume_ID, clock, available_IOPS)"
						+ "		values" + "	(?, ?, ?, ?);",
				Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.volume.getBackend().getExperiment()
				.getID());

		statement.setBigDecimal(2, this.volume.getID());

		statement.setBigDecimal(3, Experiment.clock);

		int currentIOPS = this.volume.getCurrentIOPS();

		statement.setInt(4, currentIOPS);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.setID(rs.getBigDecimal(1));

			System.out.println(this.toString() + " currentIOPS = "
					+ currentIOPS);

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}
}
