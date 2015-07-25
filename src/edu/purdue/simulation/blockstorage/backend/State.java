package edu.purdue.simulation.blockstorage.backend;

import java.util.Iterator;

import edu.purdue.simulation.blockstorage.Volume;

public class State {

	public State(Backend backend) {
		super();

		this.backend = backend;
	}

	private Backend backend;

	public int getAvailableCapacity() {
		return this.backend.getSpecifications().getCapacity()
				- this.getUsedCapacity();
	}

	public int getUsedCapacity() {

		int usedSpace = 0;

		for (Iterator<Volume> i = this.backend.getVolumeList().iterator(); i
				.hasNext();) {

			usedSpace += i.next().getSpecifications().getCapacity();

		}

		return usedSpace;
	}

//	public int getUsedIOPS() {
//
//		//
//
//		return 0;
//	}
//
//	public int getAvailableIOPS() {
//		return this.backend.getSpecifications().getIOPS() - this.getUsedIOPS();
//
//	}
}
