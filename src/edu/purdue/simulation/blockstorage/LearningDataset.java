package edu.purdue.simulation.blockstorage;

import weka.core.Instances;
import edu.purdue.simulation.blockstorage.backend.Backend;

public class LearningDataset {

	public LearningDataset() {

	}

	public LearningDataset(Backend backend, String violationGroup,
			int totRequestedIOPS, int numberOfVolums, int clock) {
		super();
		this.backend = backend;
		this.violationGroup = violationGroup;
		this.totRequestedIOPS = totRequestedIOPS;
		this.numberOfVolums = numberOfVolums;
		this.clock = clock;
	}
	
	public Instances instances_samples;

	public Backend backend;

	public String violationGroup;

	public int totRequestedIOPS;

	public int numberOfVolums;

	public int clock;
}
