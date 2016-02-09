import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import edu.purdue.simulation.BlockStorageSimulator;
import edu.purdue.simulation.Database;
import edu.purdue.simulation.VolumeRequest;
import edu.purdue.simulation.Workload;
import edu.purdue.simulation.blockstorage.Scheduler;

public class generateData_FeedbackComparison {

	public static void IOPS_Allocation_Report(long experiment_ID, int is_feedback, int clockMinus) throws Exception {
		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareCall("{call IOPS_Allocation_Report(?, ?, ?)}")) {

			statement.setLong(1, experiment_ID); // experiment_ID

			statement.setInt(2, is_feedback); // is_feedback

			statement.setInt(3, clockMinus); // clockMinus

			try (ResultSet rs = statement.executeQuery()) {

				while (rs.next()) {

					String temp = String.format("%d	%d	%f", //
							rs.getInt(1), // feedback
							rs.getLong(2), // clock
							rs.getFloat(3) // alloc percent rs.getDouble(3)
					);

					feedbackCompare.println(temp);
				}
			}
		}
	}

	private static PrintWriter feedbackCompare;

	public static void main(String args[]) {

		long experiment_ID = 329;

		long feedback_experiment_ID = 372;

		String name = "EfficiencyFirst_BayesianNetwork";

		try {
			generateData_FeedbackComparison.feedbackCompare = new PrintWriter(
					"D:\\Dropbox\\Research\\MLScheduler\\SAS\\Data\\" + experiment_ID + "_" + feedback_experiment_ID
							+ "_" + name + ".txt",
					"UTF-8");

			feedbackCompare.println("feedback	clock	alloc_percent");

			IOPS_Allocation_Report(feedback_experiment_ID, //
					0, // is feedback
					188// ClockMinte
			);

			IOPS_Allocation_Report(experiment_ID, //
					1, // is feedback
					196// ClockMinte
			);

			generateData_FeedbackComparison.feedbackCompare.close();
		} catch (Exception e) {

			e.printStackTrace();
		}
	}
}
