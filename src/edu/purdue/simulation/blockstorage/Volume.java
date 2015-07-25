package edu.purdue.simulation.blockstorage;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.PersistentObject;
import edu.purdue.simulation.blockstorage.backend.*;

public class Volume extends PersistentObject {

	public Volume(Backend backend, ScheduleResponse scheduleResponse,
			VolumeSpecifications specifications) {

		this.backend = backend;

		this.specifications = specifications;

		this.ScheduleResponse = scheduleResponse;

	}

	private VolumeSpecifications specifications;

	private Backend backend;

	private ScheduleResponse ScheduleResponse;

	public Backend getBackend() {
		return backend;
	}

	public ScheduleResponse getScheduleResponse() {
		return ScheduleResponse;
	}

	public VolumeSpecifications getSpecifications() {

		return this.specifications;
	}

	public int getCurrentIOPS() {
		int backEndVolumesTotalRequestedIOPS = 0;

		int numberOfVolumes = this.backend.getVolumeList().size();

		for (int i = 0; i < numberOfVolumes; i++) {

			Volume volume = this.backend.getVolumeList().get(i);

			backEndVolumesTotalRequestedIOPS += volume.specifications.getIOPS();
		}

		int backendCurrentAvailableIOPS = this.backend.getSpecifications()
				.getIOPS();

		if (backEndVolumesTotalRequestedIOPS > backendCurrentAvailableIOPS) {

			return Math.round(backendCurrentAvailableIOPS / numberOfVolumes);

		} else {
			// volume SLA IOPS + (available IOPS of the backend)
			return this.specifications.getIOPS()
					+ (backendCurrentAvailableIOPS - backEndVolumesTotalRequestedIOPS);
		}
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"Insert Into BlockStorageSimulator.volume"
								+ "	(backend_ID, schedule_response_ID, capacity, IOPS, is_deleted)"
								+ "		values" + "	(?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.backend.getID());

		statement.setBigDecimal(2, this.ScheduleResponse == null ? null
				: this.ScheduleResponse.ID);

		statement.setInt(3, this.specifications.getCapacity());

		statement.setInt(4, this.specifications.getIOPS());

		statement.setBoolean(5, this.specifications.IsDeleted);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.setID(rs.getBigDecimal(1));

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}

}
