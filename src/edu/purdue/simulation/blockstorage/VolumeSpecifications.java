package edu.purdue.simulation.blockstorage;

public class VolumeSpecifications extends Specifications {

	public VolumeSpecifications(int capacity, int IOPS, int latency,
			boolean isDeleted) {
		super(capacity, IOPS, latency);

		this.IsDeleted = isDeleted;
	}

	public boolean IsDeleted;
}
