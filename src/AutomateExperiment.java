import java.util.concurrent.TimeUnit;
import org.apache.tools.ant.types.Commandline;
import edu.purdue.simulation.blockstorage.AssessmentPolicy;
import edu.purdue.simulation.blockstorage.MachineLearningAlgorithm;

public class AutomateExperiment {

	public static String buildPath = //
	"java -cp \"D:\\Dropbox\\WorkSpaceLuna\\MLBlockStorage\\target\\classes"
			+ ";" + "D:\\Dropbox\\Research\\MLScheduler\\jar\\libs\\*\" "
			+ "edu.purdue.simulation.BlockStorageSimulator ";

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
				Runtime.getRuntime().exec(runCommand);

				TimeUnit.SECONDS.sleep(120);

			} catch (Exception e) {

				System.out.println("*******ERROR******");
				e.printStackTrace();
				System.out.println("*******ERROR******");

			}
		}
	}

	public static String baseCommand = //
	"-isTraining false -trainingExperimentID 124 -assessmentPolicy QoSFirst "
			+ "-machineLearningAlgorithm BayesianNetwork -feedBackLearning false "
			+ "-feedbackLearningInterval 200 -modClockBy 300 "
			+ "-StochasticEventGenerator.clockGapProbability 250 -workloadID 161 "
			+ "-devideDeleteFactorBy 2.5 -maxRequest 10000 -startTestDatasetFromRecordRank 10000";

	public static void runForAllAssessmentPolicies(
			MachineLearningAlgorithm alg, boolean feedBackLearning) {
		// AutomateExperiment.experimentDesign q;
		new experimentDesign(baseCommand,//
				new String[] {//
				"assessmentPolicy",//
						AssessmentPolicy.StrictQoS.toString(),
						//
						"machineLearningAlgorithm",//
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
				}).run();

		new experimentDesign(baseCommand,//
				new String[] {//
				"assessmentPolicy",//
						AssessmentPolicy.QoSFirst.toString(),
						//
						"machineLearningAlgorithm",//
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
				}).run();

		new experimentDesign(baseCommand,//
				new String[] {//
				"assessmentPolicy",//
						AssessmentPolicy.EfficiencyFirst.toString(),
						//
						"machineLearningAlgorithm",//
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
				}).run();

		new experimentDesign(baseCommand,//
				new String[] {//
				"assessmentPolicy",//
						AssessmentPolicy.MaxEfficiency.toString(),
						//
						"machineLearningAlgorithm",//
						alg.toString(),
						//
						"feedBackLearning", String.valueOf(feedBackLearning)
				//
				}).run();
	}

	public static void main(String[] args) {

		runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, true);

		runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork,
				true);

		runForAllAssessmentPolicies(MachineLearningAlgorithm.J48, false);

		runForAllAssessmentPolicies(MachineLearningAlgorithm.BayesianNetwork,
				false);

	}
}
