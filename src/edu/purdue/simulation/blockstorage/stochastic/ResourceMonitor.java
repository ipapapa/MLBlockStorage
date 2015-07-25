package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.SQLException;

import edu.purdue.simulation.Experiment;
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

	private final int clockGap = 3; // every 3 times measure resources
									// performance

	public ResourceMonitor() {

	}

	private int clock = 1;

	public void run() {
		// while (true) {
		if (this.clock == this.clockGap) {

			this.clock = 1; // reset clock

			for (int i = 0; i < Experiment.BackEndList.size(); i++) {

				Backend backend = Experiment.BackEndList.get(i);

				BackendPerformanceMeter backendPerformanceMeter = new BackendPerformanceMeter(
						backend);

				Volume pingVolume = null;

				try {
					pingVolume = backend.createPingVolumeThenSave();

					backendPerformanceMeter.Save(pingVolume);

					backend.removeVolume(pingVolume);

				} catch (SQLException e) {

					e.printStackTrace();
				}

				for (int j = 0; j < backend.getVolumeList().size(); j++) {

					Volume volume = backend.getVolumeList().get(j);

					VolumePerformanceMeter volumePerformanceMeter = new VolumePerformanceMeter(
							volume);

					try {
						volumePerformanceMeter.Save();
					} catch (SQLException e) {

						e.printStackTrace();
					}
				}
			}
		}

		this.clock++;

		// }
	}
}
