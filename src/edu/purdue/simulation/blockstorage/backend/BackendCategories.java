package edu.purdue.simulation.blockstorage.backend;

public enum BackendCategories {

	None(0, 0, 0), //
	VeryStable(0.0, 0.1, 1), //
	Stable(0.1, 0.2, 2), //
	Unstable(0.2, 0.4, 3), //
	VeryUnstable(0.4, 1.0, 4); //

	public final double std;

	public final double mean;

	public final int order;

	BackendCategories(double std, double avg, int order) {
		this.std = std;

		this.mean = avg;

		this.order = order;
	}

	public static BackendCategories get(int order) {
		switch (order) {
		case 1:

			return VeryStable;

		case 2:

			return Stable;

		case 3:

			return Unstable;

		case 4:

			return VeryUnstable;
		}

		return None;
	}

}
