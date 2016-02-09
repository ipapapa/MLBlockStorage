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
		// return "[VOLUME PERFORMANCE] clock = " + Experiment.clock
		// + " exp_ID = " + this.backend.getExperiment().getID()
		// + " SLAViolation = " + SLAViolation + " ID = " + this.getID()
		// + " volume_ID = "
		// + (this.volume == null ? " NULL " : this.volume.getID())
		// + " backendID = " + this.backend.getID();

		return "[VOLUME PERFORMANCE] clock = " + Experiment.clock //
				+ " SLAViolation = " + SLAViolation //
				+ " volume_ID = " + (this.volume == null ? " NULL " : this.volume.getID()) + " backendID = "
				+ this.backend.getID();
	}

	@SuppressWarnings("unused")
	public BigDecimal save() throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection
				.prepareStatement("Insert Into BlockStorageSimulator.volume_performance_meter"
						+ "	(experiment_ID, volume_ID, clock, available_IOPS, backend_total_IOPS, SLA_violation, backend_ID)"
						+ "		values" + "	(?, ?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS)) {

			statement.setBigDecimal(1, this.backend.getExperiment().getID()); // experiment_ID

			int currentAvailableIOPS = 0;

			boolean SLAViolation = false;

			if (this.volume == null) {

				statement.setNull(2, Types.NUMERIC); // volume_ID
			} else {
				currentAvailableIOPS = this.volume.getAvailableIOPS_ForEachVolume();

				SLAViolation = currentAvailableIOPS < this.volume.getSpecifications().getIOPS();
				// backend.getAllocatedIOPS()
				/*
				 * for debug
				 */
				int totAlloc = backend.getAllocatedIOPS();
				int backendIOPS = backend.getSpecifications().getIOPS();
				int volNum = backend.getVolumeList().size();// backend.getSpecifications().getIOPS()
				int clock = Experiment.clock.intValue();
				int availIOPS_EachVol = this.volume.getAvailableIOPS_ForEachVolume();
				int vol_requestedIOPS = this.volume.getSpecifications().getIOPS();

				if (SLAViolation == false) {
					// this.backend.getVolumeList().size()

					if (backend.getVolumeList().size() >= 5) {

						availIOPS_EachVol = this.volume.getAvailableIOPS_ForEachVolume();
					}
				} else {
					int vio = 1;
				}

				statement.setBigDecimal(2, this.volume.getID()); // volume_ID
			}

			statement.setBigDecimal(3, Experiment.clock); // clock

			statement.setInt(4, currentAvailableIOPS); // available_IOPS

			statement.setInt(5, this.backend.getSpecifications().getIOPS()); // backend_total_IOPS

			statement.setBoolean(6, SLAViolation); // SLA_violation

			statement.setBigDecimal(7, this.backend.getID()); // backend_ID

			VolumePerformanceMeter.queries.add(Database.getQuery(statement));

			edu.purdue.simulation.BlockStorageSimulator.log(this.toString(SLAViolation));

			// statement.executeUpdate();
			//
			// ResultSet rs = statement.getGeneratedKeys();
			//
			// if (rs.next()) {
			//
			// this.setID(rs.getBigDecimal(1));
			//
			// edu.purdue.simulation.BlockStorageSimulator.log(this.toString(SLAViolation)
			// + " currentIOPS = "
			// + currentIOPS);
			//
			// return this.getID();
			// }
		}
		return BigDecimal.valueOf(-1);
	}

	public String getSaveQuery() throws SQLException, Exception {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement("Insert Into BlockStorageSimulator.volume_performance_meter"
						+ "	(experiment_ID, volume_ID, clock, available_IOPS, SLA_violation, backend_ID)"
						+ "		values" + "	(?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.backend.getExperiment().getID());

		int currentIOPS = 0;

		boolean SLAViolation = false;

		if (this.volume == null) {

			statement.setNull(2, Types.NUMERIC);
		} else {
			currentIOPS = this.volume.getAvailableIOPS_ForEachVolume();

			SLAViolation = currentIOPS < this.volume.getSpecifications().getIOPS();

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
