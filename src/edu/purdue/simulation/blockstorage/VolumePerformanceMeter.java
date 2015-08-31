package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.PersistentObject;
import edu.purdue.simulation.blockstorage.Volume;
import edu.purdue.simulation.blockstorage.backend.Backend;

public class VolumePerformanceMeter extends PersistentObject {

	public VolumePerformanceMeter(Volume volume, Backend backend) {

		this.volume = volume;

		this.backend = backend;
	}

	private Volume volume;

	private Backend backend;

	public static ArrayList<String> queries = new ArrayList<String>();

	public String toString(boolean SLAViolation) {
		return "[VOLUME PERFORMANCE] clock = " + Experiment.clock
				+ " exp_ID = " + this.backend.getExperiment().getID()
				+ " SLAViolation = " + SLAViolation + " ID = " + this.getID()
				+ " volume_ID = "
				+ (this.volume == null ? " NULL " : this.volume.getID())
				+ " backendID = " + this.backend.getID();
	}

	public BigDecimal save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"Insert Into BlockStorageSimulator.volume_performance_meter"
								+ "	(experiment_ID, volume_ID, clock, available_IOPS, SLA_violation, backend_ID)"
								+ "		values" + "	(?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.backend.getExperiment().getID());

		int currentIOPS = 0;

		boolean SLAViolation = false;

		if (this.volume == null) {

			statement.setNull(2, Types.NUMERIC);
		} else {
			currentIOPS = this.volume.getCurrentIOPS();

			SLAViolation = currentIOPS < this.volume.getSpecifications()
					.getIOPS();

			statement.setBigDecimal(2, this.volume.getID());
		}

		statement.setBigDecimal(3, Experiment.clock);

		statement.setInt(4, currentIOPS);

		statement.setBoolean(5, SLAViolation);

		statement.setBigDecimal(6, this.backend.getID());

		VolumePerformanceMeter.queries.add(Database.getQuery(statement));
		
//		statement.executeUpdate();
//
//		ResultSet rs = statement.getGeneratedKeys();
//
//		if (rs.next()) {
//
//			this.setID(rs.getBigDecimal(1));
//
//			System.out.println(this.toString(SLAViolation) + " currentIOPS = "
//					+ currentIOPS);
//
//			return this.getID();
//		}

		return BigDecimal.valueOf(-1);
	}

	public String getSaveQuery() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"Insert Into BlockStorageSimulator.volume_performance_meter"
								+ "	(experiment_ID, volume_ID, clock, available_IOPS, SLA_violation, backend_ID)"
								+ "		values" + "	(?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.backend.getExperiment().getID());

		int currentIOPS = 0;

		boolean SLAViolation = false;

		if (this.volume == null) {

			statement.setNull(2, Types.NUMERIC);
		} else {
			currentIOPS = this.volume.getCurrentIOPS();

			SLAViolation = currentIOPS < this.volume.getSpecifications()
					.getIOPS();

			statement.setBigDecimal(2, this.volume.getID());
		}

		statement.setBigDecimal(3, Experiment.clock);

		statement.setInt(4, currentIOPS);

		statement.setBoolean(5, SLAViolation);

		statement.setBigDecimal(6, this.backend.getID());

		String q = statement.toString();

		q = q.substring(q.indexOf(':') + 2, q.length());

		return q;
	}
}
