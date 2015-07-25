package edu.purdue.simulation.blockstorage.stochastic;

import java.sql.SQLException;
import java.util.Random;
import edu.purdue.simulation.blockstorage.backend.Backend;

public class DecreaseIOPSEvent extends
		edu.purdue.simulation.blockstorage.stochastic.StochasticEvent {

	Random random = new Random();

	int[] sizes = new int[4];

	public DecreaseIOPSEvent() {
		sizes[0] = -10;
		sizes[1] = -20;
		sizes[2] = -30;
		sizes[3] = -40;
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
	public void fire(Backend backend) throws SQLException {
		StochasticEvent.apply(this, backend, false);
	}

}
