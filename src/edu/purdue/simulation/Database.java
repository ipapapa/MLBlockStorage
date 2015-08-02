package edu.purdue.simulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
				.getConnection("jdbc:mysql://localhost/BlockStorageSimulator?"
						+ "user=root&password=1234");

		// } catch (SQLException ex) {
		// handle any errors
		// System.out.println("SQLException: " + ex.getMessage());
		// System.out.println("SQLState: " + ex.getSQLState());
		// System.out.println("VendorError: " + ex.getErrorCode());
		// }

		return CurrentConnection;
	}
}
