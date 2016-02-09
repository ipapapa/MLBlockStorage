import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;

public class statementToQueryConvertor {
	public static void main(String[] args) throws Exception {
		try {
			Connection connection = Database.getConnection();

			try (PreparedStatement statement = connection
					.prepareStatement("Insert Into BlockStorageSimulator.volume_performance_meter"
							+ "	(experiment_ID, volume_ID, clock, available_IOPS, SLA_violation, backend_ID)"
							+ "		values" + "	(?, ?, ?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS)) {

				statement.setBigDecimal(1, BigDecimal.valueOf(1));

				statement.setNull(2, Types.NUMERIC);

				statement.setBigDecimal(3, Experiment.clock);

				statement.setInt(4, 20);

				statement.setBoolean(5, true);

				statement.setBigDecimal(6, BigDecimal.valueOf(1));

				statement.addBatch();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}
}
