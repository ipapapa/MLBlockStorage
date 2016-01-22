package edu.purdue.simulation.blockstorage.backend;

import java.math.BigDecimal;

import edu.purdue.simulation.Experiment;

public class LVM extends Backend {

	public LVM(BigDecimal ID) {
		super(ID);
	}

	public LVM(Experiment experiment, String description,
			BackEndSpecifications specifications) {
		super(experiment, description, specifications);

	}

}
