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
import edu.purdue.simulation.blockstorage.backend.*;

public class Volume extends PersistentObject {

	public Volume(Backend backend, ScheduleResponse scheduleResponse, VolumeSpecifications specifications) {

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

	@SuppressWarnings("unused")
	public int getAvailableIOPS_ForEachVolume() {
		int backEndVolumesTotalRequestedIOPS = 0;

		int numberOfVolumes = this.backend.getVolumeList().size();

		int backendCurrentAvailableIOPS = this.backend.getSpecifications().getIOPS();

		if (true) {

			double normalizer = 0;

			if (numberOfVolumes >= 3) {

				int temp = (numberOfVolumes - 3);

				// normalizer = temp * 2; //avali in bood Ex 120 - vio rate:
				// 0.8769220113754272

				// if (temp % 2 != 0)
				//
				// temp++;
				//
				// normalizer = temp / 2.0;

				// normalizer = temp / 1.5; // 123 0.9065758585929871

				//
				normalizer = numberOfVolumes / 2.0; // ex 124 0.9065694212913513
			}

			return (int) (backendCurrentAvailableIOPS / ((double) numberOfVolumes + normalizer));

		} else { // lazmem nisst sakhtesh koni
			for (int i = 0; i < numberOfVolumes; i++) {

				Volume volume = this.backend.getVolumeList().get(i);

				backEndVolumesTotalRequestedIOPS += volume.specifications.getIOPS();
			}

			if (backEndVolumesTotalRequestedIOPS >= backendCurrentAvailableIOPS) {

				return Math.round(backendCurrentAvailableIOPS / numberOfVolumes);

			} else {
				// volume SLA IOPS + (available IOPS of the backend)
				return this.specifications.getIOPS() + (backendCurrentAvailableIOPS - backEndVolumesTotalRequestedIOPS);
			}
		}
	}

	public BigDecimal save() throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement("Insert Into BlockStorageSimulator.volume"
				+ "	(backend_ID, schedule_response_ID, capacity, IOPS, is_deleted, Delete_Probability)" + "		values"
				+ "	(?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS)) {

			statement.setBigDecimal(1, this.backend.getID());

			statement.setBigDecimal(2, this.ScheduleResponse == null ? null : this.ScheduleResponse.ID);

			statement.setInt(3, this.specifications.getCapacity());

			statement.setInt(4, this.specifications.getIOPS());

			statement.setBoolean(5, this.specifications.IsDeleted);

			statement.setDouble(6, this.specifications.deleteFactor);

			statement.executeUpdate();

			try (ResultSet rs = statement.getGeneratedKeys()) {

				if (rs.next()) {

					this.setID(rs.getBigDecimal(1));

					return this.getID();
				}
			}
		}

		return BigDecimal.valueOf(-1);
	}

	public void delete() throws SQLException, Exception {
		this.getSpecifications().IsDeleted = true;

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement(
				"Update	volume	set	is_deleted	= 1, delete_clock = ?	Where	ID	= ?",
				Statement.RETURN_GENERATED_KEYS)) {

			statement.setBigDecimal(1, Experiment.clock);
			statement.setBigDecimal(2, this.getID());

			statement.executeUpdate();

			this.backend.getVolumeList().remove(this);
		}
	}

	public String toString() {
		return this.toString(-10);
	}

	public String toString(double deleteFactor) {

		String result = "[VOLUME] ID: " + this.getID() + " deleteProbability: " + deleteFactor;

		if (this.ScheduleResponse != null)

			result += " scheduleResponseID= " + this.ScheduleResponse.ID;

		return result + this.getSpecifications().toString();
	}

}
