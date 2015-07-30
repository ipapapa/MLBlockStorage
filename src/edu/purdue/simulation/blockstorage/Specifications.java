package edu.purdue.simulation.blockstorage;

import edu.purdue.simulation.PersistentObject;

public abstract class Specifications extends PersistentObject {

	// total_bytes_sec: the total allowed bandwidth for the guest per second
	// read_bytes_sec: sequential read limitation
	// write_bytes_sec: sequential write limitation
	// total_iops_sec: the total allowed IOPS for the guest per second
	// read_iops_sec: random read limitation
	// write_iops_sec: random write limitation

	public Specifications() {

	}

	public Specifications(int capacity, int IOPS, int latency) {

		super();

		this.Capacity = capacity;

		this.IOPS = IOPS;

		this.Latency = latency;

	}

	private int Capacity;

	private int IOPS;

	private int Latency;

	public int getCapacity() {
		return Capacity;
	}

	public void setCapacity(int capacity) {
		Capacity = capacity;
	}

	public int getIOPS() {
		return IOPS;
	}

	public void setIOPS(int iOPS) {
		IOPS = iOPS;
	}

	public int getLatency() {
		return Latency;
	}

	public void setLatency(int latency) {
		Latency = latency;
	}

	@Override
	public String toString() {
		return String.format("Capacity: %d - IOPS: %d - Latency: %d - ",
				this.Capacity, this.IOPS, this.Latency);
	}
}
