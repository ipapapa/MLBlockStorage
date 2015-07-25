package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.*;
import edu.purdue.simulation.blockstorage.backend.Backend;

public class ScheduleResponse {

	public ScheduleResponse(Experiment experiment, VolumeRequest volumeRequest) {
		super();

		this.volumeRequest = volumeRequest;

		this.experiment = experiment;
	}

	public VolumeRequest volumeRequest;

	public BigDecimal ID;

	public Experiment experiment;

	public Backend backEndCreated;

	public Backend backEndTurnedOn;

	public Backend backEndScheduled;

	public boolean isSuccessful;

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert	Into	BlockStorageSimulator.schedule_response"
								+ "		(experiment_id, volume_request_ID, backend_turned_on, backend_create, backend_scheduled, is_successful, clock)"
								+ "			values" + "		(?, ?, ?, ?, ?, ?, ?)",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.experiment.getID());

		statement.setBigDecimal(2, this.volumeRequest.getID());

		statement.setBigDecimal(3, this.backEndTurnedOn == null ? null
				: this.backEndTurnedOn.getID());

		statement.setBigDecimal(4, this.backEndCreated == null ? null
				: this.backEndCreated.getID());

		statement.setBigDecimal(5, this.backEndScheduled == null ? null
				: this.backEndScheduled.getID());

		statement.setBoolean(6, this.isSuccessful);

		statement.setBigDecimal(7, Experiment.clock);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.ID = rs.getBigDecimal(1);

			return this.ID;
		}

		return BigDecimal.valueOf(-1);

	}

}
