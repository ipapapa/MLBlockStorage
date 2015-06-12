package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;

import edu.purdue.simulation.blockstorage.GroupSize;
import edu.purdue.simulation.blockstorage.backend.BackEnd;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.LVM;

public class Experiment {

	public Experiment(String comment, String schedulerAlgorithm) {
		super();
		Comment = comment;
		SchedulerAlgorithm = schedulerAlgorithm;

		this.BackEndList = new ArrayList<>();
	}

	public BigDecimal ID;

	public String Comment;

	public String SchedulerAlgorithm;

	public ArrayList<BackEnd> BackEndList;

	public ArrayList<BackEnd> GenerateBackEnd() throws SQLException {

		if (this.ID == null || !(this.ID.compareTo(BigDecimal.ZERO) > 0))

			this.Save();

		BackEndList = new ArrayList<BackEnd>();

		BackEndSpecifications[] specifications = {
				new BackEndSpecifications(200, 1000, 20, true),
				new BackEndSpecifications(400, 1000, 20, true),
				new BackEndSpecifications(1000, 1000, 20, true),
				new BackEndSpecifications(1200, 1000, 20, true),
				new BackEndSpecifications(1000, 1000, 20, true),
				new BackEndSpecifications(1000, 1000, 20, true),
				new BackEndSpecifications(1000, 1000, 20, true), };

		for (int i = 0; i < specifications.length; i++) {
			BackEnd backEnd = new LVM(this, specifications[i]);

			backEnd.Save();

			BackEndList.add(backEnd);
		}

		return BackEndList;

	}

	public BackEnd AddBackEnd(int size, GroupSize groupSize)
			throws SQLException {
		BackEnd backEnd = this.AddBackEnd(size);

		backEnd.setGroupSize(groupSize);

		return backEnd;
	}

	public BackEnd AddBackEnd(int size) throws SQLException {

		BackEnd backEnd = new LVM(this, new BackEndSpecifications(size, //
				1000, 20, true));

		backEnd.Save();

		BackEndList.add(backEnd);

		return backEnd;
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(
				"insert into experiment " + "	(comment, scheduler_algorithm)"
						+ "		Values" + "	(?, ?)",
				Statement.RETURN_GENERATED_KEYS);

		statement.setString(1, this.Comment);

		statement.setString(2, this.SchedulerAlgorithm);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			this.ID = rs.getBigDecimal(1);

			return this.ID;
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public String toString() {
		return String.format("ID: %d - comment: %d - SchedulerAlgorithm: %d",
				this.ID.toString(), this.Comment, this.SchedulerAlgorithm);
	}
}
