package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import edu.purdue.simulation.*;
import edu.purdue.simulation.blockstorage.backend.Backend;

public class ScheduleResponse {

	/*
	 * the whole concept is very difficault to implement, not going to be used
	 */
	public enum RejectionReason {

		none(-1), IOPS(1), Capacity(2), IOPS_Capacity(3);

		RejectionReason(int ID) {
			this.ID = ID;
		}

		public final int ID;
	}

	public ScheduleResponse(Experiment experiment, VolumeRequest volumeRequest) {
		super();

		this.volumeRequest = volumeRequest;

		this.experiment = experiment;
	}

	public static ArrayList<String> queries = new ArrayList<>();

	// public RejectionReason rejectionReason;

	public VolumeRequest volumeRequest;

	public BigDecimal ID;

	public Experiment experiment;

	public Backend backEndCreated;

	public Backend backEndTurnedOn;

	public Backend backEndScheduled;

	public boolean isSuccessful;

	@SuppressWarnings("unused")
	public BigDecimal save() throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection
				.prepareStatement(
						"insert	Into	BlockStorageSimulator.schedule_response"
								+ "		(experiment_id, volume_request_ID, backend_turned_on, backend_create, backend_scheduled, is_successful, clock, rejection_reason_ID)"
								+ "			values" + "		(?, ?, ?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS)) {

			statement.setBigDecimal(1, this.experiment.getID()); // experiment_id

			statement.setBigDecimal(2, this.volumeRequest.getID()); // volume_request_ID

			statement.setBigDecimal(3, this.backEndTurnedOn == null ? null : this.backEndTurnedOn.getID()); // backend_turned_on

			statement.setBigDecimal(4, this.backEndCreated == null ? null : this.backEndCreated.getID()); // backend_create

			statement.setBigDecimal(5, this.backEndScheduled == null ? null : this.backEndScheduled.getID()); // backend_scheduled

			statement.setBoolean(6, this.isSuccessful); // is_successful

			statement.setBigDecimal(7, Experiment.clock); // clock

			statement.setNull(8, Types.INTEGER); // rejection_reason_ID

			// if (this.rejectionReason == null
			// || this.rejectionReason == RejectionReason.none)
			//
			// statement.setNull(8, Types.INTEGER);
			//
			// else
			//
			// statement.setInt(8, rejectionReason.ID);

			if (this.isSuccessful) {

				statement.executeUpdate();

				try (ResultSet rs = statement.getGeneratedKeys()) {

					if (rs.next()) {

						this.ID = rs.getBigDecimal(1);

						return this.ID;
					}
				}
			} else {
				ScheduleResponse.queries.add(Database.getQuery(statement));

			}
		}

		return BigDecimal.valueOf(-1);

	}

	@SuppressWarnings("unused")
	public BigDecimal saveWithVolume(Volume volume) throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareCall( //
				"{call Insert_schedule_response_then_volume(?,?,?,?,?,?,?,?,?,?,?,?,?)}")) {

			statement.setBigDecimal(1, this.experiment.getID()); // experiment_id

			statement.setBigDecimal(2, this.volumeRequest.getID()); // volume_request_ID

			statement.setBigDecimal(3, this.backEndTurnedOn == null ? null : this.backEndTurnedOn.getID()); // backend_turned_on

			statement.setBigDecimal(4, this.backEndCreated == null ? null : this.backEndCreated.getID()); // backend_create

			statement.setBigDecimal(5, this.backEndScheduled == null ? null : this.backEndScheduled.getID()); // backend_scheduled

			statement.setBoolean(6, this.isSuccessful); // is_successful

			statement.setBigDecimal(7, Experiment.clock); // clock

			statement.setNull(8, Types.INTEGER); // rejection_reason_ID

			int num = 8;

			/* FOR VOLUME */

			statement.setBigDecimal(1 + num, volume.getBackend().getID()); // backend_ID

			statement.setInt(2 + num, volume.getSpecifications().getCapacity()); // capacity

			statement.setInt(3 + num, volume.getSpecifications().getIOPS()); // IOPS

			statement.setBoolean(4 + num, volume.getSpecifications().IsDeleted); // is_deleted

			statement.setDouble(5 + num, volume.getSpecifications().deleteFactor); // Delete_Probability

			// if (this.rejectionReason == null
			// || this.rejectionReason == RejectionReason.none)
			//
			// statement.setNull(8, Types.INTEGER);
			//
			// else
			//
			// statement.setInt(8, rejectionReason.ID);

			if (this.isSuccessful) {
				statement.execute();

				try (ResultSet rs = statement.getResultSet()) {

					if (rs.next()) {

						this.ID = rs.getBigDecimal(1); //

						volume.setID(rs.getBigDecimal(2)); // volume_ID
					}
				}
			}
		}

		return BigDecimal.valueOf(-1);

	}
}
