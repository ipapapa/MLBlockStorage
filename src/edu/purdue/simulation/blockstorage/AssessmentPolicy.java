package edu.purdue.simulation.blockstorage;

public enum AssessmentPolicy {
	StrictQoS, //
	QoSFirst, //
	EfficiencyFirst, //
	MaxEfficiency;

	public static AssessmentPolicy parse(String input) {

		switch (input) {
		case "StrictQoS":

			return StrictQoS;

		case "QoSFirst":

			return QoSFirst;

		case "EfficiencyFirst":

			return EfficiencyFirst;

		case "MaxEfficiency":

			return MaxEfficiency;
		}

		return null;
	}
}
