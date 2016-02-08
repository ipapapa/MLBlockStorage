import java.io.Console;
import java.util.concurrent.TimeUnit;

import org.apache.tools.ant.types.Commandline;
import org.joda.time.DateTime;

import edu.purdue.simulation.blockstorage.AssessmentPolicy;
import edu.purdue.simulation.blockstorage.MachineLearningAlgorithm;

public class AutomateExperiment {

	public static String buildPath = // -Xms512M -Xmx1024M
	" java -d64 -Xms1024M -Xmx1024M -cp \"D:\\Dropbox\\WorkSpaceMars\\MLBlockStorage\\target\\classes" + ";"
			+ "D:\\Dropbox\\Research\\MLScheduler\\jar\\libs\\*\" " + "edu.purdue.simulation.BlockStorageSimulator ";

	private static class experimentDesign {

		public experimentDesign(String baseCommand, String[] replaceCommands) {
			// CommandLineParser parser; = new org.apache.commons.();

			myArgs = Commandline.translateCommandline(baseCommand);

			for (int i = 0; i < replaceCommands.length; i++) {

				if (!replaceCommands[i].startsWith("-")) {
					replaceCommands[i] = "-" + replaceCommands[i];
				}

				for (int j = 0; j < myArgs.length; j++) {

					if (myArgs[j].compareTo(replaceCommands[i]) == 0) {

						myArgs[j + 1] = replaceCommands[i + 1];

						break;
					}
				}
			}
		}

		String myArgs[];

		public void run() {

			String runCommand = AutomateExperiment.buildPath;

			for (String arg : this.myArgs) {
				runCommand += " " + arg;
			}

			try {
				counter++;

				System.out.println("Counter: " + counter + "-" + DateTime.now() + " - " + runCommand);

				// TimeUnit.MINUTES.sleep(5);

				// Runtime rt = Runtime.getRuntime();

				// rt.exec("cmd.exe /c start " + runCommand, null, null);
				// /*cmd.exe /c start */

				Process p = Runtime.getRuntime().exec(runCommand);

				p.waitFor();

				System.out.println("Done ! " + counter + "-" + DateTime.now());

				// Runtime.getRuntime().exec("cmd.exe /c" +runCommand);

				// Runtime.getRuntime().exec("cmd.exe /c ping 4.2.2.4");
				// Runtime.getRuntime().exec("cmd");

			} catch (Exception e) {

				System.out.println("*******ERROR2******");
				e.printStackTrace();
				System.out.println("*******ERROR2******");

			}
		}
	}

	public static int counter = 0;

	public static String baseCommand = //
	"-isTraining false -trainingExperimentID 400 -assessmentPolicy xx "
			+ "-machineLearningAlgorithm xx -feedBackLearning xx "
			// + "-feedbackLearningInterval 180 -modClockBy 300 "
			// + "-StochasticEventGenerator.clockGapProbability 140 -workloadID
			// 161 "
			// +
			// "-devideDeleteFactorBy 2.5 -maxRequest 10000
			// -startTestDatasetFromRecordRank 10000"
	;

	public static void runForAllAssessmentPolicies(MachineLearningAlgorithm alg, boolean feedBackLearning) {
		// AutomateExperiment.experimentDesign q;

		new experimentDesign(baseCommand, //
				new String[] { //
						"assessmentPolicy", //
						AssessmentPolicy.StrictQoS.toString(),
						//
						"machineLearningAlgorithm", //
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
		}).run();

		new experimentDesign(baseCommand, //
				new String[] { //
						"assessmentPolicy", //
						AssessmentPolicy.QoSFirst.toString(),
						//
						"machineLearningAlgorithm", //
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
		}).run();

		new experimentDesign(baseCommand, //
				new String[] { //
						"assessmentPolicy", //
						AssessmentPolicy.EfficiencyFirst.toString(),
						//
						"machineLearningAlgorithm", //
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
		}).run();

		// new experimentDesign(baseCommand,//
		// new String[] {//
		// "assessmentPolicy",//
		// AssessmentPolicy.MaxEfficiency.toString(),
		// //
		// "machineLearningAlgorithm",//
		// alg.toString(),
		// //
		// "feedBackLearning", String.valueOf(feedBackLearning)
		// //
		// }).run();
	}

	public static void main(String[] args) {

		for (int i = 0; i < 10; i++) {

			runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, true);

			runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork, true);

		}

		// for (int i = 0; i < 10; i++) {
		//
		// runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, false);
		//
		// runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork,
		// false);
		// }

	}
}
