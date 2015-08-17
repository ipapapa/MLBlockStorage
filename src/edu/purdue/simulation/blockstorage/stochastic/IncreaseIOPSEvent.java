package edu.purdue.simulation.blockstorage.stochastic;

import java.sql.SQLException;
import java.util.Random;

import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.State;

public class IncreaseIOPSEvent extends
		edu.purdue.simulation.blockstorage.stochastic.StochasticEvent {

	Random random = new Random();

	int[] sizes = new int[1];

	public IncreaseIOPSEvent() {
		sizes[0] = 1;

	}

	@Override
	public Random getRandom() {
		return this.random;
	}

	@Override
	protected int[] getSizes() {
		return this.sizes;
	}

	@Override
	public int getEventType() {
		return 3;
	}

	@Override
	public void fire(Backend backend) throws SQLException {
		StochasticEvent.apply(this, backend, false);
	}

}
