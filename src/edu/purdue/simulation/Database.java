package edu.purdue.simulation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Database {

	private static Connection CurrentConnection;

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
