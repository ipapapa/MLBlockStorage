package edu.purdue.simulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.sun.org.apache.xpath.internal.operations.Bool;

import edu.purdue.simulation.blockstorage.Scheduler;

public class Database {

	private static Connection CurrentConnection;

	public static ResultSet executeQuery(String query, Object... args) throws SQLException, Exception {
		Connection connection = getConnection();

		try (PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {

			for (int i = 0; i < args.length; i++) {
				statement.setObject(i + 1, args[i]);
			}

			return statement.executeQuery();
		}
	}

	public static String getQuery(Statement statement) {
		String q = statement.toString();

		if (q.endsWith(";") == false)

			q = q + ";";

		q = q.substring(q.indexOf(':') + 2, q.length() - 1);

		return q;
	}

	private static int compareSize = 1000;

	public static void executeBatchQuery(ArrayList<String> queries, boolean forceSave) throws SQLException, Exception {

		int queriesSize = queries.size();

		boolean save = false;

		if (Scheduler.feedBackLearning) {
			if (Experiment.clock.intValue() % Scheduler.feedbackLearningInterval == 0)

				save = true;

		} else if (queriesSize >= compareSize)

			save = true;

		save = save || forceSave;

		if (save == false || queries.size() == 0)

			return;

		Connection connection = Database.getConnection();

		try (Statement statement = connection.createStatement()) {

			String allQueris = "";

			boolean isFirst = true;

			for (int i = 0; i < queriesSize; i++) {
				// statement.addBatch(queries.get(i));

				String q = queries.get(i);

				q = q.toLowerCase();
				
				if (isFirst == true) {
					allQueris += q + "\n";

					isFirst = false;
				} else {
					allQueris += "," + q.substring(q.indexOf("values") + 6, q.length());
				}
			}

			// if (allQueris.endsWith(",")) {
			// allQueris = allQueris.substring(0, allQueris.length() - 1);
			// }

			// statement.executeBatch();

			statement.executeUpdate(allQueris);

			queries.clear();
		}
	}

	public static Connection getConnection() throws SQLException, Exception {

		if (CurrentConnection != null && !CurrentConnection.isClosed())

			return CurrentConnection;
		
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("com.mysql.jdbc.Driver");
		
		CurrentConnection = DriverManager.getConnection(
				"jdbc:mysql://localhost:3306/blockstoragesimulator?user=root&useServerPrepStmts=false&rewriteBatchedStatements=true&allowMultiQueries=true",
				"root", "123");
		
//		CurrentConnection = DriverManager.getConnection(
//				"jdbc:jdbc:mysql://localhost:3306/blockstoragesimulator?user=root&useServerPrepStmts=false&rewriteBatchedStatements=true&allowMultiQueries=true",
//				"root", "234");

		return CurrentConnection;
	}
}
