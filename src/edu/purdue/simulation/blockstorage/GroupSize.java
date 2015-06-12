package edu.purdue.simulation.blockstorage;

public enum GroupSize {

	None(0, 0, 0, 0, 0), Small(-1.0, 0, 0, 1.0, 1), //
	Medium(-1.8, -1.0, 1.0, 1.8, 2), //
	Large(-2.6, -1.8, 1.8, 2.6, 3), //
	XLarge(-3.4, -2.6, 2.6, 3.4, 4), //
	XXLarge(-5.4, -4.6, 4.6, 5.4, 5), //
	XXXLarge(0, 0, 0, 0, 6);

	public final double lowerBound1;

	public final double upperBound1;

	public final double lowerBound2;

	public final double upperBound2;

	public final int order;

	GroupSize(double lowerBound1, double upperBound1, double lowerBound2,
			double upperBound2, int order) {
		this.lowerBound1 = lowerBound1;

		this.upperBound1 = upperBound1;

		this.lowerBound2 = lowerBound2;

		this.upperBound2 = upperBound2;

		this.order = order;
	}

	public static GroupSize get(int order) {
		switch (order) {
		case 1:

			return Small;

		case 2:

			return Medium;

		case 3:

			return Large;

		case 4:

			return XLarge;

		case 5:

			return XXLarge;

		case 6:

			return XXXLarge;
		}

		return None;
	}

}
