package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.SQLException;

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
public class ResourceMonitor implements Runnable {

	public static int clockGap = 2; // every 3 times measure resources
									// performance

	public static boolean enableBackendPerformanceMeter = false;

	public static boolean enableVolumePerformanceMeter = false;

	public static int devideVolumeDeleteProbability = 3;

	public static boolean recordVolumePerformanceForClocksWithNoVolume = false;

	public ResourceMonitor() {

	}

	private int clock = 0;

	public void run() {
		// while (true) {

		if (this.clock == ResourceMonitor.clockGap)

			this.clock = 0; // reset clock

		this.clock++;

		for (int i = 0; i < Experiment.backendList.size(); i++) {

			Backend backend = Experiment.backendList.get(i);

			if (ResourceMonitor.enableBackendPerformanceMeter
					&& this.clock == ResourceMonitor.clockGap) {

				BackendPerformanceMeter backendPerformanceMeter = new BackendPerformanceMeter(
						backend);

				Volume pingVolume = null;

				try {
					pingVolume = backend.createPingVolume();

					pingVolume.save();

					backendPerformanceMeter.Save(pingVolume);

					backend.removeVolume(pingVolume);

				} catch (SQLException e) {

					e.printStackTrace();
				}
			}

			// for now no need to read all volumes performance
			if (ResourceMonitor.enableVolumePerformanceMeter
					&& this.clock == ResourceMonitor.clockGap) {

				if (ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume
						&& backend.getVolumeList().size() == 0) {
					VolumePerformanceMeter volumePerformanceMeter = new VolumePerformanceMeter(
							null, backend);

					try {
						volumePerformanceMeter.Save();
					} catch (SQLException e) {

						e.printStackTrace();
					}

				} else {

					for (int j = 0; j < backend.getVolumeList().size(); j++) {

						Volume volume = backend.getVolumeList().get(j);

						VolumePerformanceMeter volumePerformanceMeter = new VolumePerformanceMeter(
								volume, backend);

						try {
							volumePerformanceMeter.Save();
						} catch (SQLException e) {

							e.printStackTrace();
						}
					}
				}
			}

		}

		// }
	}
}
