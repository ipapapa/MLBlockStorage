package edu.purdue.simulation.blockstorage;

public class VolumeSpecifications extends Specifications {

	public VolumeSpecifications(int capacity, int IOPS, int latency,
			boolean isDeleted, double deleteProbability) {
		super(capacity, IOPS, latency);

		this.deleteProbability = deleteProbability;

		this.IsDeleted = isDeleted;
	}

	public double deleteProbability;

	public boolean IsDeleted;
	
	public String toString(){
		return super.toString() + " isDeleted: " + this.IsDeleted + " deleteProbability: " + this.deleteProbability;
	}
}
