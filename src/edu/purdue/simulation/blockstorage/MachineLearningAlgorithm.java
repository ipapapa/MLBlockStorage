package edu.purdue.simulation.blockstorage;

public enum MachineLearningAlgorithm {
	RepTree, Regression, J48, BayesianNetwork;

	public static MachineLearningAlgorithm parse(String input) {

		switch (input) {
		case "RepTree":

			return RepTree;

		case "Regression":

			return Regression;

		case "J48":

			return J48;

		case "BayesianNetwork":

			return BayesianNetwork;
		}

		return null;
	}
}
