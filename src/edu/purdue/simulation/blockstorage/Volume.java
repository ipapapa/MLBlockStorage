package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.blockstorage.backend.*;

public class Volume {

	public Volume(BackEnd backEnd, ScheduleResponse scheduleResponse,
			VolumeSpecifications specifications) {
		this.BackEnd = backEnd;

		this.Specifications = specifications;

		this.ScheduleResponse = scheduleResponse;
		
	}

	private VolumeSpecifications Specifications;

	private BackEnd BackEnd;

	public ScheduleResponse ScheduleResponse;

	public BigDecimal ID;

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"Insert Into BlockStorageSimulator.volume"
								+ "	(backend_ID, schedule_response_ID, capacity, IOPS, is_deleted)"
								+ "		values" + "	(?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.BackEnd.ID);

		statement.setBigDecimal(2, this.ScheduleResponse.ID);

		statement.setInt(3, this.Specifications.getCapacity());

		statement.setInt(4, this.Specifications.getIOPS());

		statement.setBoolean(5, this.Specifications.IsDeleted);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.ID = rs.getBigDecimal(1);

			return this.ID;
		}

		return BigDecimal.valueOf(-1);
	}

	public VolumeSpecifications getSpecifications() {
		// TODO Auto-generated method stub
		return this.Specifications;
	}
}
