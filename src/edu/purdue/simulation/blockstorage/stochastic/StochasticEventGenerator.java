package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;

import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.backend.Backend;

/**
 * @author ravandi This class generates unexpected events that could happen in a
 *         BlockStorage backend such as hard drive added or removed. The events
 *         are generated using pseudo random generators with specific
 *         distributions.
 * 
 *         The goal is use the data captured from ResourceMonitor class in order
 *         to classify which event(s) is happened in the system since the
 *         backends are considered as blackbox. Later on, this classification
 *         could help scheduling volume request with better performance
 */

public class StochasticEventGenerator implements Runnable {

	public StochasticEventGenerator() {
		events = new ArrayList<StochasticEvent>();

		events.add(new IncreaseCapacityEvent());

		events.add(new DecreaseCapacityEvent());

		events.add(new IncreaseIOPSEvent());

		events.add(new DecreaseIOPSEvent());

		this.clock = 1;
	}

	private final int clockGap = 4; // every 4 times
									// cause
									// an event
	private int clock;

	public void run() {

		// while (true) {

		if (this.clock == this.clockGap) {

			this.clock = 1;

			Backend target = this.selectRandomBackEnd();

			StochasticEvent event = this.events.get(this.eventRandom
					.nextInt(this.events.size()));

			try {
				event.fire(target);
			} catch (SQLException e) {
				e.printStackTrace();
			}

		}

		this.clock++;

		// }
	}

	private ArrayList<StochasticEvent> events;

	Random eventRandom = new Random();

	private Backend selectRandomBackEnd() {

		Random random = new Random();

		int backendSize = Experiment.backEndList.size();

		if (backendSize == 0)

			return null;

		int randomNumber = random.nextInt(backendSize - 0) + 0;

		return Experiment.backEndList.get(randomNumber);
	}
}