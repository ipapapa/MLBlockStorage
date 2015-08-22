package edu.purdue.simulation.blockstorage.backend;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

import weka.classifiers.AbstractClassifier;
import weka.classifiers.trees.REPTree;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.PersistentObject;
import edu.purdue.simulation.blockstorage.*;

/**
 * @author ravandi
 *
 */

public abstract class Backend extends PersistentObject {

	public Backend(Experiment experiment, String desciption,
			BackEndSpecifications specifications) {

		this.volumeList = new ArrayList<Volume>();

		this.specifications = specifications;

		this.state = new State(this);

		this.experiment = experiment;

		this.setDescription(desciption);
	}

	public REPTree repTree;

	public void createRepTree(String params) {
		String arguments = params;
		// "-t D:\\SAS\\2\\514Cat_g3.arff -M 2 -V 0.001 -N 3 -S 1 -L -1 -c 3"

		this.repTree = new REPTree();

		AbstractClassifier.runClassifier(this.repTree, arguments.split(" "));
	}

	public static void createTrainingDataForRepTree(int numberOfRecords,
			Experiment experiment, Backend backend, String path)
			throws java.lang.Exception {

		Connection connection = Database.getConnection();

		CallableStatement cStmt = connection
				.prepareCall("{call data_for_ML(?, ?, ?, ?)}"); // ex_ID,
																// backend_ID
																// ,lim
																// ,modBy

		cStmt.setBigDecimal(1, experiment.getID());

		cStmt.setBigDecimal(2, backend.getID());

		cStmt.setInt(3, numberOfRecords);

		cStmt.setInt(4, 1440); // each day is 1440

		cStmt.execute();

		ResultSet rs = cStmt.getResultSet();

		Attribute clockAttribute = new Attribute("clock");
		Attribute numAttribute = new Attribute("num");

		FastVector<String> fvClassVal = new FastVector<String>(2);

		fvClassVal.addElement("v1");
		fvClassVal.addElement("v2");
		fvClassVal.addElement("v3");
		fvClassVal.addElement("v4");

		Attribute violationNumbersAttribute = new Attribute("vio", fvClassVal);

		Attribute totalRequestedIOPSAttribute = new Attribute("tot");

		FastVector<Attribute> attributesVector = new FastVector<Attribute>(5);

		attributesVector.addElement(clockAttribute); // 0
		attributesVector.addElement(numAttribute); // 1
		attributesVector.addElement(violationNumbersAttribute); // 2
		attributesVector.addElement(totalRequestedIOPSAttribute); // 3

		Instances trainingInstances = new Instances("Rel", attributesVector, 10);

		trainingInstances.setClassIndex(3); // total 4 attributes

		while (rs.next()) {
			Instance trainingInstance = new DenseInstance(4);

			trainingInstance.setValue(clockAttribute, rs.getBigDecimal(1)
					.doubleValue());

			trainingInstance.setValue(numAttribute, rs.getInt(2));

			int numberOfViolations = rs.getInt(3);

			String group = "";

			if (numberOfViolations == 0) {
				group = "v1";
			} else if (numberOfViolations > 0 && numberOfViolations <= 2) {
				group = "v2";
			} else if (numberOfViolations > 2 && numberOfViolations <= 4) {
				group = "v3";
			} else {
				group = "v4";
			}

			trainingInstance.setValue(violationNumbersAttribute, group);

			trainingInstance
					.setValue(totalRequestedIOPSAttribute, rs.getInt(4));

			// add the instance
			trainingInstances.add(trainingInstance);
		}

		ArffSaver saver = new ArffSaver();

		saver.setInstances(trainingInstances);

		String saveToPath = "";

		if (path == null || path == "")

			saveToPath = "D:\\Research\\experiment\\"
					+ backend.getExperiment().getID() + "_" + backend.getID()
					+ "_" + backend.getDescription() + ".arff";

		else

			saveToPath = path;

		saver.setFile(new File(saveToPath));

		// saver.setDestination(new File(path));

		saver.writeBatch();
		// Create the instance

		// System.out.println(Arrays.toString(repTree
		// .distributionForInstance(iExample)));
	}

	private String description;

	private Experiment experiment;

	private State state;

	private BackEndSpecifications specifications;

	private List<Volume> volumeList;

	public Experiment getExperiment() {
		return experiment;
	}

	public BackEndSpecifications getSpecifications() {
		return specifications;
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

	public String getDescription() {

		String path = this.getSpecifications().getTrainingDataSetPath();

		String result = "";

		if (Scheduler.isTraining) {

			result = "_training_";

			if (path != null && path != "") {
				String[] pathParts = this.getSpecifications()
						.getTrainingDataSetPath().split("\\");

				if (pathParts.length > 0)

					result += pathParts[pathParts.length - 1];
			}
		}

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

		int c = Experiment.clock.intValue() % 24;

		double result = 403.78735 + (0.30922 * c) - (115.01801 * n)
				+ (10.05588 * (n ^ 2));

		c = Experiment.clock.intValue();

		// double result2 = 403.78735 + (0.30922 * c) - (115.01801 * n)
		// + (10.05588 * (n ^ 2));
		//
		// if(c > 80){
		// System.out.println(result2);
		// }

		return (int) result;
	}

	public void createRegressionModel() throws SQLException {

		Connection connection = Database.getConnection();

		CallableStatement statement = connection
				.prepareCall("{Call data_for_regression(?, ?, ?)}");

		statement.setBigDecimal(1, this.experiment.getID());

		statement.setBigDecimal(2, this.getID());

		statement.setInt(3, 0);

		ResultSet resultset = statement.executeQuery();

		int rowcount = 0;

		if (resultset.last()) {
			rowcount = resultset.getRow();

			resultset.beforeFirst();
		}

		double[] y = new double[rowcount];

		double[][] x = new double[rowcount][3];

		int i = 0;

		while (resultset.next()) {

			x[i][0] = resultset.getDouble(1) % 24; // clock
			x[i][1] = resultset.getDouble(2); // num
			x[i][2] = x[i][1] * x[i][1]; // num ^ 2

			y[i] = resultset.getDouble(3);

			i++;
		}

		final OLSMultipleLinearRegression reg = new OLSMultipleLinearRegression();

		reg.newSampleData(y, x);

		double[] beta = reg.estimateRegressionParameters();

		double r2 = reg.calculateRSquared();

		PreparedStatement statement2 = connection.prepareStatement(
				"insert into backend_regression"
						+ "	(backend_ID, clock, R2, Description)" + "		Values"
						+ "	(?, ?, ?, ?);", Statement.RETURN_GENERATED_KEYS);

		statement2.setBigDecimal(1, this.getID());

		statement2.setBigDecimal(2, Experiment.clock);

		statement2.setDouble(3, r2);

		String description = String.format(
				"y = %f + (%f * clock) + (%f * num) + (%f * num2)", beta[0],
				beta[1], beta[2], beta[3]);

		statement2.setString(4, description);

		statement2.executeUpdate();
	}

	private BigDecimal doSave(boolean isUpdate, int operationID)
			throws SQLException {
		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert into backend"
								+ "	(experiment_id, capacity, IOPS, is_online, clock, MaxCapacity, MinCapacity, MaxIOPS, MinIOPS, operation_ID, Description, stability_possession_mean)"
								+ "		Values"
								+ "	(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
						Statement.RETURN_GENERATED_KEYS);

		statement.setBigDecimal(1, this.experiment.getID());

		statement.setInt(2, this.specifications.getCapacity());

		statement.setInt(3, this.specifications.getIOPS());

		statement.setBoolean(4, this.specifications.getIsOnline());

		statement.setBigDecimal(5, edu.purdue.simulation.Experiment.clock);

		statement.setInt(10, operationID);

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

			statement.setDouble(12,
					this.specifications.getStabilityPossessionMean());
		}

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			// Don't screw current IF
			if (isUpdate) {

				return rs.getBigDecimal(1);

			} else {
				this.setID(rs.getBigDecimal(1));

				return this.getID();
			}
		}

		return BigDecimal.valueOf(-1);
	}

	public BigDecimal save() throws SQLException {

		return this.doSave(false, 1); // 1 is create
	}

	/**
	 * @return creates a record in the backend table and will put null into
	 *         MaxCapacity, MinCapacity, MaxIOPS and MinIOPS columns. Then
	 *         returns that record number. NOTE this function will not change
	 *         this object ID.
	 * @throws SQLException
	 */
	public BigDecimal saveCurrentState() throws SQLException {

		return this.doSave(true, 2); // 2 save current state
	}

	public State getState() {

		return this.state;
	}

	public boolean removeVolume(Volume volume) {

		this.volumeList.remove(volume);

		return true;
	}

	public Volume createVolumeThenAdd(
			VolumeSpecifications volumeSpecifications,
			ScheduleResponse scheduleResponse, boolean isPingVolume)
			throws SQLException {

		if (isPingVolume) {

			// TODO actually implement delete volume which update the field
			// is_deleted
			volumeSpecifications = new VolumeSpecifications(0, 0, 0, true, -1,
					Experiment.clock.intValue());

			scheduleResponse = null;

		} else if (this.getState().getAvailableCapacity() < volumeSpecifications
				.getCapacity()) {
			return null;
		}

		Volume result = new Volume(this, scheduleResponse, volumeSpecifications);

		this.volumeList.add(result);

		return result;
	}

	public Volume createPingVolume() throws SQLException {

		return this.createVolumeThenAdd(null, null, true);
	}

	public Volume createVolumeThenAdd(
			VolumeSpecifications volumeSpecifications,
			ScheduleResponse scheduleResponse) throws SQLException {

		return this.createVolumeThenAdd(volumeSpecifications, scheduleResponse,
				false);
	}

	@Override
	public String toString() {
		return String
				.format("ID: %s - Capacity: %d - IOPS: %d - IsOnline: %b - Latency: %d",
						this.getID().toString(),
						this.specifications.getCapacity(),
						this.specifications.getIOPS(),
						this.specifications.getIsOnline(),
						this.specifications.getLatency());
	}

}
