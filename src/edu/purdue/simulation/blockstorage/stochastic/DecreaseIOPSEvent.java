package edu.purdue.simulation.blockstorage.stochastic;

import java.sql.SQLException;
import java.util.Random;

import edu.purdue.simulation.blockstorage.backend.Backend;

public class DecreaseIOPSEvent extends
		edu.purdue.simulation.blockstorage.stochastic.StochasticEvent {

	Random random = new Random();

	int[] sizes = new int[1];

	public DecreaseIOPSEvent() {
		sizes[0] = -1;
		// sizes[1] = -200;
		// sizes[2] = -250;
		// sizes[3] = -350;
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
		return 4;
	}

	@Override
	public void fire(Backend backend) throws SQLException, Exception {
		StochasticEvent.apply(this, backend, false);
	}

}
