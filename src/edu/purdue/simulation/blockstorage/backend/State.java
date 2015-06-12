package edu.purdue.simulation.blockstorage.backend;

public class State {
	
	
	public State(int capacity) {
		super();
		
		Capacity = capacity;
	}

	protected int UsedSpace;
	
	protected int Capacity;
	
	public int getAvailableCapacity() {
		return this.Capacity - this.UsedSpace;
	}

	public int getCapacity() {
		return Capacity;
	}

	public int getUsedSpace(){
		return this.UsedSpace;
	}
}
