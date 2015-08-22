package edu.purdue.simulation.blockstorage;

public class VolumeSpecifications extends Specifications {

	public VolumeSpecifications(int capacity, int IOPS, int latency,
			boolean isDeleted, double deleteFactor, int createClock) {
		super(capacity, IOPS, latency);

		this.deleteFactor = deleteFactor;

		this.IsDeleted = isDeleted;

		this.createClock = createClock;
	}

	public double deleteFactor;

	public boolean IsDeleted;

	public int createClock;

	public String toString() {
		return super.toString() + " isDeleted: " + this.IsDeleted
				+ " deleteFactor: " + this.deleteFactor;
	}
}
