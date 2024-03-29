package edu.purdue.simulation.blockstorage.backend;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import weka.classifiers.AbstractClassifier;
import weka.classifiers.bayes.BayesNet;
import weka.classifiers.bayes.net.estimate.BayesNetEstimator;
import weka.classifiers.bayes.net.search.SearchAlgorithm;
import weka.classifiers.evaluation.Evaluation;
import weka.classifiers.trees.J48;
import weka.classifiers.trees.REPTree;
import weka.core.Instances;
import edu.purdue.simulation.BlockStorageSimulator;
import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.PersistentObject;
import edu.purdue.simulation.blockstorage.*;

/**
 * @author ravandi
 *
 */

public abstract class Backend extends PersistentObject {

	public Backend(BigDecimal ID) {
		this.setID(ID);
	}

	public Backend(Experiment experiment, String desciption, BackEndSpecifications specifications) {

		this.volumeList = new ArrayList<Volume>();

		this.specifications = specifications;

		this.state = new State(this);

		this.experiment = experiment;

		this.setDescription(desciption);
	}

	public weka.classifiers.trees.J48 j48;

	public weka.classifiers.bayes.BayesNet bayesianNetwork;

	public REPTree repTree;

	public Evaluation classifierEvaluation;

	public void createRepTree(String params) {
		String arguments = params;
		// "-t D:\\SAS\\2\\514Cat_g3.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c 3"

		this.repTree = new REPTree();

		AbstractClassifier.runClassifier(this.repTree, arguments.split(" "));
	}

	public double updateModel(Instances instances) throws Exception {

		switch (this.specifications.getMachineLearningAlgorithm()) {

		case RepTree:

			break;

		case J48:

			// String params, String path, int classIndex, Instances train

			return this.createJ48Tree(
					// "-t %s -M 2 -V 0.001 -N 3 -S 1 -L -1 -c %d", //
					null, // TODO fix this function inputs
					null, //
					0, //
					instances//
			// no feedback learning/update model
			);

		case BayesianNetwork:

			// String params, String path, int classIndex, Instances train

			return this.createBayesianNetwork(
					// "-t %s -M 2 -V 0.001 -N 3 -S 1 -L -1 -c %d", //
					null, // TODO fix this function inputs
					null, //
					0, //
					instances//
			// no feedback learning/update model
			);

		default:

			throw new Exception("specifies machine learning algorithm is not implemented");
		}

		return 0;
	}

	@SuppressWarnings("unused")
	public double createJ48Tree(String params, String path, int classIndex, Instances train) throws Exception {

		if (false) {
			String arguments = params;

			this.j48 = new J48();

			AbstractClassifier.runClassifier(this.j48, arguments.split(" "));
			String tos = this.j48.toString();
		}

		if (true) {

			if (train == null) {

				BufferedReader reader;

				reader = new BufferedReader(new FileReader(path));

				train = new Instances(reader);

				reader.close();
			}

			// setting class attribute
			train.setClassIndex(0);

			this.j48 = null;
			System.gc();

			this.j48 = new J48();

			this.j48.setUnpruned(true);

			this.j48.buildClassifier(train);

			if (BlockStorageSimulator.validateLearningModels) {

				this.classifierEvaluation = new Evaluation(train);

				Random rand = new Random(1); // using seed = 1

				int folds = 10;

				this.classifierEvaluation.crossValidateModel(this.j48, train, folds, rand);

				// train.size()
				edu.purdue.simulation.BlockStorageSimulator.log(this.classifierEvaluation.toClassDetailsString());

				edu.purdue.simulation.BlockStorageSimulator.log("Accuracy: " + this.classifierEvaluation.pctCorrect()
						+ " Sample Size: " + train.size() + " backendIndex: " + Experiment.backendList.indexOf(this));

				return this.classifierEvaluation.pctCorrect();
			}
			// FilteredClassifier fc = new FilteredClassifier();

			// fc.setFilter(rm);

			// fc.setClassifier(j48);

			// fc.buildClassifier(train);

			// "-t D:\\SAS\\2\\514Cat_g3.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c
			// 3"

		}

		return 0;
	}

	@SuppressWarnings("unused")
	public double createBayesianNetwork(String params, String path, int classIndex, Instances train) throws Exception {

		if (true) {

			if (train == null) {

				BufferedReader reader;

				reader = new BufferedReader(new FileReader(path));

				train = new Instances(reader);

				reader.close();
			}

			// setting class attribute
			train.setClassIndex(0);

			// Scheme:weka.classifiers.bayes.BayesNet -D -Q
			// weka.classifiers.bayes.net.search.local.K2 --
			// -P 1 -S BAYES -E
			// weka.classifiers.bayes.net.estimate.SimpleEstimator -- -A 0.5
			// train.size()

			this.bayesianNetwork = null;
			System.gc();

			this.bayesianNetwork = new BayesNet();
			this.bayesianNetwork.setOptions(weka.core.Utils.splitOptions(
					"-D -Q weka.classifiers.bayes.net.search.local.K2 -- -P 1 -S BAYES -E weka.classifiers.bayes.net.estimate.SimpleEstimator -- -A 0.5"));

			// BayesNetEstimator bayesNetEstimator = new Wek
			// bayesNetEstimator.setAlpha(0.5);
			//
			// this.bayesianNetwork.setEstimator(bayesNetEstimator);
			//
			// SearchAlgorithm searchAlgorithm = new
			// weka.classifiers.bayes.net.search.local.K2();
			// searchAlgorithm.setOptions(
			// weka.core.Utils.splitOptions("-P 1 -S BAYES -E"));
			//
			// this.bayesianNetwork.setSearchAlgorithm(searchAlgorithm);

			this.bayesianNetwork.buildClassifier(train);

			if (BlockStorageSimulator.validateLearningModels) {

				this.classifierEvaluation = new Evaluation(train);

				Random rand = new Random(1); // using seed = 1

				int folds = 10;

				this.classifierEvaluation.crossValidateModel(this.bayesianNetwork, train, folds, rand);

				edu.purdue.simulation.BlockStorageSimulator.log(this.classifierEvaluation.toClassDetailsString());

				edu.purdue.simulation.BlockStorageSimulator.log("Accuracy: " + this.classifierEvaluation.pctCorrect()
						+ " Sample Size: " + train.size() + " backendIndex: " + Experiment.backendList.indexOf(this));

				return this.classifierEvaluation.pctCorrect();
			}
		}

		return 0;
	}

	private String description;

	private Experiment experiment;

	private State state;

	private BackEndSpecifications specifications;

	private List<Volume> volumeList;

	public Experiment getExperiment() {
		return this.experiment;
	}

	public void setExperiment(Experiment experiment) {
		this.experiment = experiment;

	}

	public BackEndSpecifications getSpecifications() {
		return this.specifications;
	}

	public void setSpecifications(BackEndSpecifications backEndSpecifications) {
		this.specifications = backEndSpecifications;
	}

	// I cant understand why VolumeRequestCategories is needed here ?!!

	// private VolumeRequestCategories GroupSize;

	// public VolumeRequestCategories getGroupSize() {
	// return GroupSize;
	// }
	//
	// public void setGroupSize(VolumeRequestCategories groupSize2) {
	// GroupSize = groupSize2;
	// }

	public int getAllocatedIOPS() {
		int allocated_IOPS = 0;

		for (Volume v : this.getVolumeList())

			allocated_IOPS = allocated_IOPS + v.getSpecifications().getIOPS();

		return allocated_IOPS;
	}

	public String getDescription() {

		// String path = this.getSpecifications().getTrainingDataSetPath();

		String result = "";

		// if (Scheduler.isTraining) {
		//
		// result = "_training_";
		//
		// if (path != null && path != "") {
		// String[] pathParts = this.getSpecifications()
		// .getTrainingDataSetPath().split("\\");
		//
		// if (pathParts.length > 0)
		//
		// result += pathParts[pathParts.length - 1];
		// }
		// }

		return description + result + "_StabilityPossessionMean_"
				+ this.getSpecifications().getStabilityPossessionMean();
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<Volume> getVolumeList() {
		return volumeList;
	}

	public int getAvailableIOPSWithRegression() {
		int n = this.getVolumeList().size();

		int c = Experiment.clock.intValue() % Scheduler.modClockBy;

		double result = 403.78735 + (0.30922 * c) - (115.01801 * n) + (10.05588 * (n ^ 2));

		c = Experiment.clock.intValue();

		// double result2 = 403.78735 + (0.30922 * c) - (115.01801 * n)
		// + (10.05588 * (n ^ 2));
		//
		// if(c > 80){
		// edu.purdue.simulation.BlockStorageSimulator.log(result2);
		// }

		return (int) result;
	}

	public void createRegressionModel() throws Exception {

		Connection connection = Database.getConnection();

		CallableStatement statement = connection.prepareCall("{Call data_for_regression(?, ?, ?)}");

		statement.setBigDecimal(1, this.experiment.getID());

		statement.setBigDecimal(2, this.getID());

		statement.setInt(3, 0);

		try (ResultSet resultset = statement.executeQuery()) {

			int rowcount = 0;

			if (resultset.last()) {
				rowcount = resultset.getRow();

				resultset.beforeFirst();
			}

			double[] y = new double[rowcount];

			double[][] x = new double[rowcount][3];

			int i = 0;

			while (resultset.next()) {

				x[i][0] = resultset.getDouble(1) % Scheduler.modClockBy; // clock
				x[i][1] = resultset.getDouble(2); // num
				x[i][2] = x[i][1] * x[i][1]; // num ^ 2

				y[i] = resultset.getDouble(3);

				i++;
			}

			final OLSMultipleLinearRegression reg = new OLSMultipleLinearRegression();

			reg.newSampleData(y, x);

			double[] beta = reg.estimateRegressionParameters();

			double r2 = reg.calculateRSquared();

			try (PreparedStatement statement2 = connection.prepareStatement("insert into backend_regression"
					+ "	(backend_ID, clock, R2, Description)" + "		Values" + "	(?, ?, ?, ?);",
					Statement.RETURN_GENERATED_KEYS)) {

				statement2.setBigDecimal(1, this.getID());

				statement2.setBigDecimal(2, Experiment.clock);

				statement2.setDouble(3, r2);

				String description = String.format("y = %f + (%f * clock) + (%f * num) + (%f * num2)", beta[0], beta[1],
						beta[2], beta[3]);

				statement2.setString(4, description);

				statement2.executeUpdate();
			}
		}
	}

	private BigDecimal doSave(boolean isUpdate, int operationID) throws SQLException, Exception {
		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement(
				"insert into backend"
						+ "	(experiment_id, capacity, IOPS, is_online, clock, MaxCapacity, MinCapacity, MaxIOPS, MinIOPS, operation_ID, Description, stability_possession_mean, Save_Path)"
						+ "		Values" + "	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
				Statement.RETURN_GENERATED_KEYS)) {

			statement.setBigDecimal(1, this.experiment.getID());

			statement.setInt(2, this.specifications.getCapacity());

			statement.setInt(3, this.specifications.getIOPS());

			statement.setBoolean(4, true);// this.specifications.getIsOnline());

			statement.setBigDecimal(5, edu.purdue.simulation.Experiment.clock);

			statement.setInt(10, operationID);

			if (isUpdate)

				throw new Exception(
						"Backend.save --> isUpdate is not supported. We dont get track of stochastic events anymore");

			if (isUpdate) {

				statement.setNull(6, java.sql.Types.INTEGER);

				statement.setNull(7, java.sql.Types.INTEGER);

				statement.setNull(8, java.sql.Types.INTEGER);

				statement.setNull(9, java.sql.Types.INTEGER);

				statement.setString(11, null);

				statement.setNull(12, java.sql.Types.DOUBLE);

			} else {
				statement.setInt(6, this.specifications.getMaxCapacity());

				statement.setInt(7, this.specifications.getMinCapacity());

				statement.setInt(8, this.specifications.getMaxIOPS());

				statement.setInt(9, this.specifications.getMinIOPS());

				statement.setString(11, this.getDescription());

				statement.setDouble(12, this.specifications.getStabilityPossessionMean());

				statement.setString(13, this.getSpecifications().getTrainingWorkloadPath());
			}

			statement.executeUpdate();

			try (ResultSet rs = statement.getGeneratedKeys()) {

				if (rs.next()) {

					// Don't screw current IF
					if (isUpdate) {

						return rs.getBigDecimal(1);

					} else {
						this.setID(rs.getBigDecimal(1));

						return this.getID();
					}
				}
			}
		}

		return BigDecimal.valueOf(-1);
	}

	public BigDecimal save() throws SQLException, Exception {

		return this.doSave(false, 1); // 1 is create
	}

	/**
	 * @return creates a record in the backend table and will put null into
	 *         MaxCapacity, MinCapacity, MaxIOPS and MinIOPS columns. Then
	 *         returns that record number. NOTE this function will not change
	 *         this object ID.
	 * @throws SQLException
	 */
	public BigDecimal saveCurrentState() throws SQLException, Exception {

		return this.doSave(true, 2); // 2 save current state
	}

	public State getState() {

		return this.state;
	}

	public boolean removeVolume(Volume volume) {

		this.volumeList.remove(volume);

		return true;
	}

	public Volume createVolumeThenAdd(VolumeSpecifications volumeSpecifications, ScheduleResponse scheduleResponse,
			boolean isPingVolume) throws SQLException {

		if (isPingVolume) {

			// TODO actually implement delete volume which update the field
			// is_deleted
			volumeSpecifications = new VolumeSpecifications(0, 0, 0, true, -1, Experiment.clock.intValue());

			scheduleResponse = null;

		} else if (this.getState().getAvailableCapacity() < volumeSpecifications.getCapacity()) {
			return null;
		}

		Volume result = new Volume(this, scheduleResponse, volumeSpecifications);

		this.volumeList.add(result);

		return result;
	}

	public Volume createPingVolume() throws SQLException {

		return this.createVolumeThenAdd(null, null, true);
	}

	public Volume createVolumeThenAdd(VolumeSpecifications volumeSpecifications, ScheduleResponse scheduleResponse)
			throws SQLException {

		return this.createVolumeThenAdd(volumeSpecifications, scheduleResponse, false);
	}

	@Override
	public String toString() {
		return String.format(
				"ID: %s - volCount: %d - Capacity: %d - stability: %f - IOPS: %d - IsOnline: %b - Latency: %d",
				this.getID().toString(), //
				this.getVolumeList().size(), //
				this.specifications.getCapacity(), //
				this.specifications.getStabilityPossessionMean(), this.specifications.getIOPS(), true, // this.specifications.getIsOnline(),
				this.specifications.getLatency());
	}

}
