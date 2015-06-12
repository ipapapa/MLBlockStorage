package edu.purdue.simulation.blockstorage.backend;

import edu.purdue.simulation.blockstorage.*;

public class BackEndSpecifications extends Specifications {

	public BackEndSpecifications() {

		super();

		this.Initialize();

	}

	public BackEndSpecifications(int capacity, int IOPS, int latency,
			boolean isOnline) {
		super(capacity, IOPS, latency);

		this.Initialize();
	}

	private void Initialize() {

	}

	// I have this here to calculate available space of backends from this class
	// not sure if its a good idea
	protected void setBackEnd(BackEnd backEnd) {
		this.BackEnd = backEnd;
	}

	private BackEnd BackEnd;

	public boolean IsOnline;
}