package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.Volume;
import edu.purdue.simulation.blockstorage.VolumePerformanceMeter;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackendPerformanceMeter;

/**
 * @author ravandi this class keeps calculating the current state of each volume
 *         also adds a new voume to the backends in order to calculate available
 *         IOPS for the backends the data will be stored based on time in order
 *         to use machine learning later on to classify the backend system for
 *         now all I can think is recognizing (classifying) when a backend have
 *         harddrives removed or added
 */
public class ResourceMonitor { // implements Runnable {

	public static int clockGap = 2; // every 3 times measure resources
									// performance

	public static boolean enableBackendPerformanceMeter = false;

	public static boolean enableVolumePerformanceMeter = false;

	public static int devideVolumeDeleteProbability = 3;

	public static boolean recordVolumePerformanceForClocksWithNoVolume = false;

	public ResourceMonitor() {

	}

	private int clock = 0;

	public void run() throws SQLException {
		// while (true) {

		if (this.clock == ResourceMonitor.clockGap)

			this.clock = 0; // reset clock

		this.clock++;

		if (this.clock == ResourceMonitor.clockGap) {

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

						if (backend.getVolumeList().size() > 1) {
							int q2 = 1;

							q2++;
						}

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
