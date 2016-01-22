import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;

import edu.purdue.simulation.BlockStorageSimulator;
import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.MachineLearningAlgorithm;
import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.LVM;

public class Tests {

	public static void main(String[] args) throws Exception {

		Experiment.saveResultPath = "D:\\Dropbox\\Research\\MLScheduler\\experiment\\";
		BlockStorageSimulator.logPath = "D:\\Dropbox\\Research\\MLScheduler\\experiment_console_output\\";

		Scheduler.isTraining = true;
		Experiment ex = new Experiment(BigDecimal.valueOf(70));

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(//
				"Select	ID, Capacity, MaxCapacity, Description From Backend	Where	Experiment_ID	= "
						+ ex.getID().toString());

		ResultSet rs = statement.executeQuery();

		Experiment.backendList = new ArrayList<Backend>();

		while (rs.next()) {

			Backend b = new LVM(rs.getBigDecimal(1));

			BackEndSpecifications bs = new BackEndSpecifications();
			// rs.getInt(2),//capacity
			// rs.getInt(3),//MaxCapacity
			// rs.getInt(3),//minCapacity
			// IOPS
			// maxIOPS
			// minIOPS
			// 0,//latency
			// true,//isOnline
			// stabilityPossessionMean
			/*
			 * int capacity, int maxCapacity, int minCapacity, int IOPS, int
			 * maxIOPS, int minIOPS, int latency, boolean isOnline, double
			 * stabilityPossessionMean, String trainingDataSetPath,
			 * MahineLearningAlgorithm machineLearningAlgorithm
			 */

			// );

			b.setDescription(rs.getString(4));

			b.setSpecifications(bs);

			b.setExperiment(ex);

			Experiment.backendList.add(b);
		}

		ex.createUpdateWorkload(0, ex, null, 0, //
				0 // no feedback learning/update model
		); // include all values
	}
}
