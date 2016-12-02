import java.io.BufferedReader;
import java.io.Console;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.tools.ant.types.Commandline;
import org.joda.time.DateTime;

import edu.purdue.simulation.blockstorage.AssessmentPolicy;
import edu.purdue.simulation.blockstorage.MachineLearningAlgorithm;

public class AutomateExperiment {

	public static String buildPath = // -Xms512M -Xmx1024M -d64 -Xms1024M
										// -Xmx1024M
	" java -d64 -Xmx1024M -Xms1024M -cp \"D:\\Dropbox\\WorkSpaceMars_L\\MLBlockStorage\\target\\classes" + ";"
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
				mainCounter++;

				runCommand = "cmd.exe /C start " + runCommand;

				System.out.println("Counter: " + mainCounter + "-" + DateTime.now() + " - " + runCommand);

				int currentNumberOfJavaProc = getNumberOfCurrentJavaProcesses();

				while (currentNumberOfJavaProc > 8) {

					TimeUnit.SECONDS.sleep(5);
					
					currentNumberOfJavaProc = getNumberOfCurrentJavaProcesses();

				}

				Process p = Runtime.getRuntime().exec(runCommand);

				TimeUnit.SECONDS.sleep(2);

			} catch (Exception e) {

				System.out.println("*******ERROR2******");
				e.printStackTrace();
				System.out.println("*******ERROR2******");

			}
		}
	}

	public static int getNumberOfCurrentJavaProcesses() {
		int result = 0;

		try {
			String line;

			Process p = Runtime.getRuntime().exec(System.getenv("windir") + "\\system32\\" + "tasklist.exe");

			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));

			while ((line = input.readLine()) != null) {

				if (line.startsWith("java"))

					result++;
			}

			input.close();

		} catch (Exception err) {
			err.printStackTrace();
		}

		return result;
	}

	public static int mainCounter = 0;

	public static String baseCommand = // 2730 
	"-isTraining false -trainingExperimentID 2732  -workloadID 172 -assessmentPolicy xx "
			+ "-machineLearningAlgorithm xx -feedBackLearning xx "
			// + "-feedbackLearningInterval 180 -modClockBy 300 "
			// + "-StochasticEventGenerator.clockGapProbability 140
			// +
			// "-devideDeleteFactorBy 2.5 -maxRequest 10000
			// -startTestDatasetFromRecordRank 10000"
	;

	public static void runForAllAssessmentPolicies(MachineLearningAlgorithm alg, boolean feedBackLearning) {
		// AutomateExperiment.experimentDesign q;

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
						AssessmentPolicy.StrictQoS.toString(),
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

		// for (int i = 0; i < 10; i++) {
		//
		// runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, true);
		//
		// runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork,
		// true);
		//
		// }

		for (int i = 0; i < 100; i++) {

			runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, false);

			runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork, true);

			runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork, false);

			runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, true);

		}

	}
}
