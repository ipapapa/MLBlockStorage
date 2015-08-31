package edu.purdue.simulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class Database {

	private static Connection CurrentConnection;

	public static ResultSet executeQuery(String query, Object... args)
			throws SQLException {
		Connection connection = getConnection();

		PreparedStatement statement = connection.prepareStatement(query,
				Statement.RETURN_GENERATED_KEYS);

		for (int i = 0; i < args.length; i++) {
			statement.setObject(i + 1, args[i]);
		}

		return statement.executeQuery();
	}

	public static String getQuery(Statement statement) {
		String q = statement.toString();

		q = q.substring(q.indexOf(':') + 2, q.length() - 1);

		return q;
	}

	public static void executeBatchQuery(ArrayList<String> queries,
			int compareSize) throws SQLException {

		int size = queries.size();

		if (size == 0)

			return;

		if (compareSize > 0) {
			if (size < compareSize)

				return;
		}

		Connection connection = Database.getConnection();

		Statement statement = connection.createStatement();

		String allQueris = "";

		boolean isFirst = true;

		for (int i = 0; i < size; i++) {
			// statement.addBatch(queries.get(i));

			String q = queries.get(i);

			if (isFirst == true) {
				allQueris += q + "\n";

				isFirst = false;
			} else {
				allQueris += ","
						+ q.substring(q.indexOf("values") + 7, q.length());
			}
		}

		// if (allQueris.endsWith(",")) {
		// allQueris = allQueris.substring(0, allQueris.length() - 1);
		// }

		// statement.executeBatch();

		statement.executeUpdate(allQueris);

		queries.clear();
	}

	public static Connection getConnection() throws SQLException {

		if (CurrentConnection != null && !CurrentConnection.isClosed())

			return CurrentConnection;

		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// try {
		CurrentConnection = DriverManager
				.getConnection(
						"jdbc:mysql://localhost/BlockStorageSimulator?useServerPrepStmts=false&rewriteBatchedStatements=true",
						"root", "1234");
		// + "user=root&password=1234");

		// } catch (SQLException ex) {
		// handle any errors
		// System.out.println("SQLException: " + ex.getMessage());
		// System.out.println("SQLState: " + ex.getSQLState());
		// System.out.println("VendorError: " + ex.getErrorCode());
		// }

		return CurrentConnection;
	}
}
