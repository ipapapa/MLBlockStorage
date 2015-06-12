package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.*;
import edu.purdue.simulation.blockstorage.backend.BackEnd;

public class ScheduleResponse {

	public ScheduleResponse(Experiment experiment, VolumeRequest volumeRequest) {
		super();

		this.VolumeRequest = volumeRequest;

		this.Experiment = experiment;
	}

	public VolumeRequest VolumeRequest;

	public BigDecimal ID;

	public Experiment Experiment;

	public BackEnd BackEndCreated;

	public BackEnd BackEndTurnedOn;

	public BackEnd BackEndScheduled;

	public boolean IsSuccessful;

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert	Into	BlockStorageSimulator.schedule_response"
								+ "		(experiment_id, volume_request_ID, backend_turned_on, backend_create, backend_scheduled, is_successful)"
								+ "			values" + "		(?, ?, ?, ?, ?, ?)",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.Experiment.ID);

		statement.setBigDecimal(2, this.VolumeRequest.getID());

		statement.setBigDecimal(3, this.BackEndTurnedOn == null ? null
				: this.BackEndTurnedOn.ID);

		statement.setBigDecimal(4, this.BackEndCreated == null ? null
				: this.BackEndCreated.ID);

		statement.setBigDecimal(5, this.BackEndScheduled == null ? null
				: this.BackEndScheduled.ID);

		statement.setBoolean(6, this.IsSuccessful);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.ID = rs.getBigDecimal(1);

			return this.ID;
		}

		return BigDecimal.valueOf(-1);

	}

}
