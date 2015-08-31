package edu.purdue.simulation.blockstorage.stochastic;

import java.sql.SQLException;
import java.util.Random;

import edu.purdue.simulation.blockstorage.backend.Backend;

public class DecreaseCapacityEvent extends
		edu.purdue.simulation.blockstorage.stochastic.StochasticEvent {

	Random random = new Random();

	int[] sizes = new int[4];

	public DecreaseCapacityEvent() {
		sizes[0] = -100;
		sizes[1] = -150;
		sizes[2] = -200;
		sizes[3] = -250;
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
		return 2;
	}

	@Override
	public void fire(Backend backend) throws SQLException {

		StochasticEvent.apply(this, backend, true);

		// int backendSize = Experiment.BackEndList.size();
		//
		// if (backendSize == 0)
		//
		// return;
		//
		// int randomNumber = this.random.nextInt(this.sizes.length - 0) + 0;
		//
		// State backEndState = backend.getState();
		//
		// int decreaseBy = this.sizes[randomNumber];
		//
		// if (backEndState.getAvailableCapacity() < decreaseBy) {
		// decreaseBy = backEndState.getAvailableCapacity() * -1;
		// }
		//
		// backend.Specifications.setCapacity(backend.Specifications.getCapacity()
		// + decreaseBy);
		//
		// super.save(backend, decreaseBy, null);
	}
}
