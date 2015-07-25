package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;

import edu.purdue.simulation.blockstorage.GroupSize;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.LVM;

public class Experiment extends PersistentObject {

	public Experiment(String comment, String schedulerAlgorithm) {
		super();
		Comment = comment;
		SchedulerAlgorithm = schedulerAlgorithm;

		Experiment.BackEndList = new ArrayList<>();

		Experiment.clock = new BigDecimal(1);
	}

	public String Comment;

	public String SchedulerAlgorithm;

	public static ArrayList<Backend> BackEndList;

	public static BigDecimal clock;

	public ArrayList<Backend> GenerateBackEnd() throws SQLException {

		if (this.getID() == null
				|| !(this.getID().compareTo(BigDecimal.ZERO) > 0))

			this.Save();

		BackEndList = new ArrayList<Backend>();

		BackEndSpecifications specification = new BackEndSpecifications(1200,
				2000, 800, 500, 800, 200, 0, true);

		BackEndSpecifications[] specifications = { specification,
				specification, specification, specification, specification,
				specification, specification, };

		for (int i = 0; i < specifications.length; i++) {
			Backend backEnd = new LVM(this, specifications[i]);

			backEnd.save();

			BackEndList.add(backEnd);
		}

		return BackEndList;

	}

	public Backend AddBackEnd(BackEndSpecifications backEndSpecifications,
			GroupSize groupSize) throws SQLException {
		Backend backEnd = this.AddBackEnd(backEndSpecifications);

		backEnd.setGroupSize(groupSize);

		return backEnd;
	}

	public Backend AddBackEnd(BackEndSpecifications backEndSpecifications)
			throws SQLException {

		Backend backEnd = new LVM(this, backEndSpecifications);

		backEnd.save();

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

			this.setID(rs.getBigDecimal(1));

			return this.getID();
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public String toString() {
		return String.format("ID: %d - comment: %d - SchedulerAlgorithm: %d",
				this.getID().toString(), this.Comment, this.SchedulerAlgorithm);
	}
}
