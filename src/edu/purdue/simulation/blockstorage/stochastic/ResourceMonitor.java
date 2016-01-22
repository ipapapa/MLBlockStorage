package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.Volume;
import edu.purdue.simulation.blockstorage.VolumePerformanceMeter;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackendPerformanceMeter;

/**
 * ping each volume and save backend stat for each clock
 * 
 * @author ravandi
 */
public class ResourceMonitor { // implements Runnable {

	// every 2 times measure
	public static double clockGapProbability = 0.5;

	public static boolean enableBackendPerformanceMeter = false;

	public static boolean enableVolumePerformanceMeter = false;

	public static int devideVolumeDeleteProbability = 3;

	public static boolean recordVolumePerformanceForClocksWithNoVolume = false;

	public static ArrayList<String> backendStat_queries = new ArrayList<String>();

	public ResourceMonitor() {

	}

	private static void SaveBackendStat(Backend backend) throws SQLException,
			Exception {
		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"INSERT INTO blockstoragesimulator.backend_stat"
								+ "	(backend_ID, clock, available_IOPS, allocated_IOPS, volume_count)"
								+ "		values" + "	(?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, backend.getID());

		statement.setBigDecimal(2, Experiment.clock);

		statement.setInt(3, backend.getSpecifications().getIOPS()); // available_IOPS

		statement.setInt(4, backend.getAllocatedIOPS());

		statement.setInt(5, backend.getVolumeList().size());

		ResourceMonitor.backendStat_queries.add(Database.getQuery(statement));
	}

	// private int clock = 0;

	@SuppressWarnings("unused")
	public void run() throws SQLException, Exception {
		// while (true) {

		// if (this.clock == ResourceMonitor.clockGapProbability)
		//
		// this.clock = 0; // reset clock
		//
		// this.clock++;

		for (Backend b : Experiment.backendList)

			SaveBackendStat(b);

		double d = Math.random();

		// if (this.clock == ResourceMonitor.clockGapProbability) {
		if (d < ResourceMonitor.clockGapProbability) {

			// PreparedStatement statement = VolumePerformanceMeter
			// .getSaveStatement();

			for (int i = 0; i < Experiment.backendList.size(); i++) {

				Backend backend = Experiment.backendList.get(i);

				/*
				 * For now save backend performance is completely not supported
				 */
				if (false && ResourceMonitor.enableBackendPerformanceMeter) {

					BackendPerformanceMeter backendPerformanceMeter = new BackendPerformanceMeter(
							backend);

					Volume pingVolume = null;

					pingVolume = backend.createPingVolume();

					pingVolume.save();

					backendPerformanceMeter.Save(pingVolume);

					backend.removeVolume(pingVolume);

				}

				// for now no need to read all volumes performance
				if (ResourceMonitor.enableVolumePerformanceMeter) {

					if (ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume
							&& backend.getVolumeList().size() == 0) {

						// VolumePerformanceMeter volumePerformanceMeter = new
						// VolumePerformanceMeter(
						// null, backend);

						// volumePerformanceMeter.save();

					} else {

						// if (backend.getVolumeList().size() > 1) {
						// int q2 = 1;
						//
						// q2++;
						// }

						for (int j = 0; j < backend.getVolumeList().size(); j++) {

							Volume volume = backend.getVolumeList().get(j);

							VolumePerformanceMeter volumePerformanceMeter = new VolumePerformanceMeter(
									volume, backend);

							volumePerformanceMeter.save();

							// q += statement.toString() + "\n";

							// q += volumePerformanceMeter.save2() + "\n\n";
						}
					}

				} // end volume performance meter
			} // end for
		}
		// }
	}
}
