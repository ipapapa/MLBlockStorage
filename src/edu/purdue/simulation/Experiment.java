package edu.purdue.simulation;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffSaver;
import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.Volume;
import edu.purdue.simulation.blockstorage.VolumeRequestCategories;
import edu.purdue.simulation.blockstorage.VolumeSpecifications;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.BackEndSpecifications;
import edu.purdue.simulation.blockstorage.backend.LVM;
import edu.purdue.simulation.blockstorage.stochastic.ResourceMonitor;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEvent;
import edu.purdue.simulation.blockstorage.stochastic.StochasticEventGenerator;

@SuppressWarnings("deprecation")
public class Experiment extends PersistentObject {

	public Experiment(BigDecimal ID) {
		this.setID(ID);
	}

	public Experiment(Workload workload, String comment, String schedulerAlgorithm) {
		super();

		this.setComment(comment);

		this.setSchedulerAlgorithm(schedulerAlgorithm);

		Experiment.backendList = new ArrayList<>();

		this.setWorkload(workload);
	}

	private String comment;

	private Workload workload;

	private String schedulerAlgorithm;

	public static ArrayList<Backend> backendList;

	public static BigDecimal clock;

	public static String saveResultPath = "";

	public static int workloadID = 0;

	public String getSchedulerAlgorithm() {
		return this.schedulerAlgorithm;
	}

	public void setSchedulerAlgorithm(String schedulerAlgorithm) {
		this.schedulerAlgorithm = schedulerAlgorithm;
	}

	public String getComment() {
		return "\n**************************************************************************\n" //
				+ (this.comment + " ").trim() //
				+ " \nScheduler.isTraining = " //
				+ Scheduler.isTraining//
				+ " \nScheduler.machineLearningAlgorithm = " //
				+ Scheduler.machineLearningAlgorithm//
				+ " \nScheduler.assessmentPolicy = " //
				+ Scheduler.assessmentPolicy//
				+ " \nScheduler.feedBackLearning = " //
				+ Scheduler.feedBackLearning//
				+ " \n~~~~:" //
				+ BlockStorageSimulator.assessmentPolicyRules.get(Scheduler.assessmentPolicy) //
				+ " \nResourceMonitor.clockGap = " + ResourceMonitor.clockGapProbability //
				+ " \nScheduler.feedBackLearningInterval = " //
				+ Scheduler.feedbackLearningInterval//
				+ " \nStochasticEventGenerator.clockGap = " //
				+ StochasticEventGenerator.clockGapProbability//
				+ " \nWorkload.ID = " //
				+ this.workload.getID()//
				+ " \nScheduler.trainingExperimentID = " //
				+ Scheduler.trainingExperimentID//
				+ " \nScheduler.maxClock = " + Scheduler.maxRequest //
				+ " \nScheduler.minRequests = " //
				+ Scheduler.minRequests//
				+ " \nScheduler.modClockBy =;" //
				+ Scheduler.modClockBy//
				+ " \nWorkload.devideDeleteFactorBy = " //
				+ Workload.devideDeleteFactorBy//
				+ " \nStochasticEvent.saveStochasticEvents = " //
				+ StochasticEvent.saveStochasticEvents //
				+ " \nResourceMonitor.enableBackendPerformanceMeter = " //
				+ ResourceMonitor.enableBackendPerformanceMeter//
				+ " \nResourceMonitor.enableVolumePerformanceMeter = " //
				+ ResourceMonitor.enableVolumePerformanceMeter//
				+ " \nResourceMonitor.recordVolumePerformanceForClocksWithNoVolume = " //
				+ ResourceMonitor.recordVolumePerformanceForClocksWithNoVolume//
				+ " \nStochasticEventGenerator.applyToAllBackends = " //
				+ StochasticEventGenerator.applyToAllBackends//
				+ " \nExperiment.saveResultPath = " //
				+ Experiment.saveResultPath//
				+ " \nScheduler.schedulePausePoissonMean = NOT USED" //
				+ Scheduler.schedulePausePoissonMean//
				+ "\n**************************************************************************";
	}

	public Workload getWorkload() {
		return workload;
	}

	public void setWorkload(Workload workload) {
		this.workload = workload;
	}

	public void setComment(String comment) {
		this.comment = comment;
	}

	public ArrayList<Backend> generateBackEnd() throws SQLException, Exception {

		if (this.getID() == null || !(this.getID().compareTo(BigDecimal.ZERO) > 0))

			this.save();

		backendList = new ArrayList<Backend>();

		BackEndSpecifications specification = new BackEndSpecifications(1200, 2000, 800, 500, 800, 200, 0, true, 10000,
				null, null);

		BackEndSpecifications[] specifications = { specification, specification, specification, specification,
				specification, specification, specification, };

		for (int i = 0; i < specifications.length; i++) {
			Backend backEnd = new LVM(this, "NO DESCRIPTION", specifications[i]);

			backEnd.save();

			backendList.add(backEnd);
		}

		return backendList;

	}

	public static Experiment resumeExperiment_NOTUSED(BigDecimal experimentID) throws SQLException, Exception {

		ResultSet experimentResultSet = Database.executeQuery("Select	*	From	experiment	Where	ID	= ?",
				experimentID);

		Experiment experiment = null;

		if (experimentResultSet.next()) {

			ResultSet workloadResultSet = Database.executeQuery(
					"Select	ID, Generate_Method, Comment	From	Workload	Where	ID	= ?",
					experimentResultSet.getBigDecimal(2));

			Workload workload = null;

			if (workloadResultSet.next()) {
				workload = new Workload(//
						experimentResultSet.getInt(2), // generateMethod
						experimentResultSet.getString(3) // comment
				);

				workload.setID(workloadResultSet.getBigDecimal(1));
			}

			experiment = new Experiment(//
					workload, // workload
					experimentResultSet.getString(2), //
					experimentResultSet.getString(3));

			experiment.setID(experimentResultSet.getBigDecimal(1));

			ResultSet lastestVolumeRequestDataSet = Database.executeQuery(
					"Select	SR.volume_request_ID	From	schedule_response	SR	Where	SR.experiment_id	=	?	Order	By	SR.ID	Desc	Limit	1",
					experimentID);

			if (lastestVolumeRequestDataSet.next()) {
				workload.RetrieveVolumeRequests(lastestVolumeRequestDataSet.getBigDecimal(1));
			}
		}

		ResultSet rs = Database.executeQuery(
				"Select	*	From	Backend	Where	MaxCapacity Is Not Null and	experiment_id	= ?", experimentID);

		while (rs.next()) {

			BackEndSpecifications backendSpecifications = new BackEndSpecifications(rs.getInt(5), // Capacity
					rs.getInt(6), // MaxCapacity
					rs.getInt(7), // Min Capacity
					rs.getInt(8), // IOPS
					rs.getInt(9), // MaxIOPS
					rs.getInt(10), // MinIOPS
					0, // latency
					rs.getBoolean(11), // isOnline
					rs.getDouble(13), //
					null, null);

			Backend backend = new LVM(//
					experiment, //
					rs.getString(12), // Description
					backendSpecifications);

			backend.setID(rs.getBigDecimal(1));

			Experiment.backendList.add(backend);
		}

		for (int i = 0; i < Experiment.backendList.size(); i++) {

			Backend backend = Experiment.backendList.get(i);

			ResultSet volumesResultSet = Database.executeQuery(
					"Select	*	From	volume	where	is_deleted = 0 and	backend_ID	= ?	order by	ID	Desc",
					backend.getID());

			while (volumesResultSet.next()) {

				VolumeSpecifications volumeSpecifications = new VolumeSpecifications(volumesResultSet.getInt(4), // capacity
						volumesResultSet.getInt(5), // IOPS
						0, // latency
						volumesResultSet.getBoolean(6), // isDeleted
						volumesResultSet.getDouble(7), //
						0 // create time
				);

				Volume volume = new Volume(backend, null, volumeSpecifications);

				volume.setID(volumesResultSet.getBigDecimal(1));

				backend.getVolumeList().add(volume);
			}
		}

		ResultSet clockResultSet = Database.executeQuery(
				"select	Clock	From	volume_performance_meter	Where	experiment_id		= ?	Order	By	ID	Desc	Limit	1",
				experimentID);

		if (clockResultSet.next()) {
			Experiment.clock = clockResultSet.getBigDecimal(1);
		}

		return experiment;

	}

	public Backend addBackEnd(String description, BackEndSpecifications backEndSpecifications,
			VolumeRequestCategories groupSize) throws Exception {

		Backend backEnd = this.addBackEnd(description, backEndSpecifications);

		// I cant understand why is needed in BackEndSpecifications for
		// statisticalGroupping method
		// backEnd.setGroupSize(groupSize);

		return backEnd;
	}

	public Backend addBackEnd(String description, BackEndSpecifications backEndSpecifications) throws Exception {

		Backend backEnd = new LVM(this, description, backEndSpecifications);

		if (Scheduler.isTraining == false
		// && backEndSpecifications.getMachineLearningAlgorithm() ==
		// MachineLearningAlgorithm.RepTree
		) {

			BufferedReader reader = new BufferedReader(new FileReader(backEndSpecifications.getTrainingDataSetPath()));

			Instances data = new Instances(reader);

			int j = -1;

			for (int i = 0; i < data.numAttributes(); i++) {

				if (data.attribute(i).name().compareTo("vio") == 0) {

					j = i;

					break;
				}

				j = -1;
			}

			if (j == -1)

				throw new Exception("Cannot find attribute vio in the training dataset");

			reader.close();
			data = null; // dispose it

			switch (backEndSpecifications.getMachineLearningAlgorithm()) {

			case RepTree:

				backEnd.createRepTree(String.format("-t %s -M 2 -V 0.001 -N 3 -S 1 -L -1 -c %d", //
						backEndSpecifications.getTrainingDataSetPath(), //
						j + 1));

				break;

			case J48:

				backEnd.createJ48Tree(
						String.format(
								// "-t %s -M 2 -V 0.001 -N 3 -S 1 -L -1 -c %d",
								// //
								"-t %s -c %d -C 0.25 -M 2", //
								backEndSpecifications.getTrainingDataSetPath(), //
								j + 1), //
						backEndSpecifications.getTrainingDataSetPath(), //
						j, //
						null // no feedback learning/update model
				);

				break;

			case BayesianNetwork:

				backEnd.createBayesianNetwork(
						String.format(
								// "-t %s -M 2 -V 0.001 -N 3 -S 1 -L -1 -c %d",
								// //
								"-t %s -c %d -C 0.25 -M 2", //
								backEndSpecifications.getTrainingDataSetPath(), //
								j + 1), //
						backEndSpecifications.getTrainingDataSetPath(), //
						j, //
						null // no feedback learning/update model
				);

				break;
			default:

				throw new Exception("specifies machine learning algorithm is not implemented");
			}
		}

		backEnd.save();

		backendList.add(backEnd);

		return backEnd;
	}

	public void update() throws SQLException, Exception {
		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement(
				"Update	experiment	Set	scheduler_algorithm	=?, comment= ?, workload_ID = ?	Where	ID	= ?",
				Statement.RETURN_GENERATED_KEYS)) {

			statement.setString(1, this.getSchedulerAlgorithm());

			statement.setString(2, this.getComment());

			statement.setBigDecimal(3, this.getWorkload().getID());

			statement.setBigDecimal(4, this.getID());

			statement.executeUpdate();
		}
	}

	@Override
	public BigDecimal save() throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement("insert into experiment "
				+ "	(comment, scheduler_algorithm, workload_ID)" + "		Values" + "	(?, ?, ?)",
				Statement.RETURN_GENERATED_KEYS)) {

			statement.setString(1, this.getComment());

			statement.setString(2, this.getSchedulerAlgorithm());

			statement.setBigDecimal(3, this.getWorkload().getID());

			statement.executeUpdate();

			try (ResultSet rs = statement.getGeneratedKeys()) {

				if (rs.next()) {

					this.setID(rs.getBigDecimal(1));

					return this.getID();
				}
			}
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public String toString() {
		return String.format("Experiment ID: %s - SchedulerAlgorithm: %s - comment: %s", this.getID().toString(),
				this.getSchedulerAlgorithm().toString(), this.getComment());
	}

	/**
	 * @param includeViolationsNumber
	 *            0: Don't include SLA violations number 1: include SLA
	 *            violations number 2: include SLA violation number and remove
	 *            violation label
	 */
	public FastVector<Attribute> getDatasetAttributes(int includeViolationsNumber) {
		FastVector<Attribute> attributesVector = new FastVector<Attribute>(4);

		Attribute clockAttribute = new Attribute("clock");

		Attribute numAttribute = new Attribute("num");

		Attribute violationGroupAttribute = null;

		if (includeViolationsNumber < 2) {
			FastVector<String> fvClassVal = new FastVector<String>(4);

			fvClassVal.addElement("v1");
			fvClassVal.addElement("v2");
			fvClassVal.addElement("v3");
			fvClassVal.addElement("v4");

			violationGroupAttribute = new Attribute("vio", fvClassVal);

			attributesVector.addElement(violationGroupAttribute); // 2
		}

		Attribute totalRequestedIOPSAttribute = new Attribute("tot");

		attributesVector.addElement(clockAttribute); // 0

		attributesVector.addElement(numAttribute); // 1

		attributesVector.addElement(totalRequestedIOPSAttribute); // 3

		Attribute violationNumberAttribute = null;

		if (includeViolationsNumber > 0) {

			violationNumberAttribute = new Attribute("vioNum");

			attributesVector.addElement(violationNumberAttribute);
		}

		return attributesVector;
	}

	public static Instances wekaDataset = null;

	/**
	 * @param includeViolationsNumber
	 *            -1: return cashed | dataset 0: Don't include SLA violations
	 *            number 1: include SLA violations number 2: include SLA
	 *            violation number and remove violation label
	 * @throws Exception
	 */
	@SuppressWarnings({})
	public static Instances createWekaDataset(int includeViolationsNumber) {

		if (includeViolationsNumber == -1 && wekaDataset != null)

			return wekaDataset;

		FastVector<Attribute> attributesVector = new FastVector<Attribute>(4);

		Attribute clockAttribute = new Attribute("clock");

		Attribute numAttribute = new Attribute("num");

		Attribute violationGroupAttribute = null;

		if (includeViolationsNumber < 2) {
			FastVector<String> fvClassVal = new FastVector<String>(4);

			fvClassVal.addElement("v1");
			fvClassVal.addElement("v2");
			fvClassVal.addElement("v3");
			fvClassVal.addElement("v4");

			violationGroupAttribute = new Attribute("vio", fvClassVal);

			attributesVector.addElement(violationGroupAttribute); // 2
		}

		Attribute totalRequestedIOPSAttribute = new Attribute("tot");

		attributesVector.addElement(clockAttribute); // 0

		attributesVector.addElement(numAttribute); // 1

		attributesVector.addElement(totalRequestedIOPSAttribute); // 3

		Attribute violationNumberAttribute = null;

		Attribute clockModAttribute = null;

		if (includeViolationsNumber > 0) {

			violationNumberAttribute = new Attribute("vioNum");

			attributesVector.addElement(violationNumberAttribute);

			clockModAttribute = new Attribute("clockMod");

			attributesVector.addElement(clockModAttribute);
		}

		Instances trainingInstances = new Instances("Rel", attributesVector, 10);

		trainingInstances.setClassIndex(0);// attributesVector.size() - 1);

		wekaDataset = trainingInstances;

		return trainingInstances;
	}

	/**
	 * @param rs
	 * @param backend
	 * @param path
	 * @param includeViolationsNumber
	 *            0: Don't include SLA violations number 1: include SLA
	 *            violations number 2: include SLA violation number and remove
	 *            violation label
	 * @throws Exception
	 */
	@SuppressWarnings({})
	private void saveBackend(ResultSet rs, Backend backend, String path, int includeViolationsNumber,
			boolean updateLearningModel) throws Exception {

		Instances trainingInstances = Experiment.createWekaDataset(includeViolationsNumber);

		while (rs.next()) {

			Instance trainingInstance = new DenseInstance(trainingInstances.numAttributes());

			double clock = rs.getBigDecimal(1).doubleValue();

			trainingInstance.setValue(trainingInstances.attribute("clock"), clock);

			trainingInstance.setValue(trainingInstances.attribute("num"), rs.getInt(2));
			// rs.getInt(1);
			int numberOfViolations = rs.getInt(3);
			// rs.last() rs.getRow()
			if (trainingInstances.attribute("vio") != null) {

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

				trainingInstance.setValue(trainingInstances.attribute("vio"), group);
			}

			trainingInstance.setValue(trainingInstances.attribute("tot"), rs.getInt(4));

			if (trainingInstances.attribute("vioNum") != null) {

				trainingInstance.setValue(trainingInstances.attribute("vioNum"), numberOfViolations);

				trainingInstance.setValue(trainingInstances.attribute("clockMod"), clock % Scheduler.modClockBy);
			}

			// add the instance
			trainingInstances.add(trainingInstance);
		}

		if (updateLearningModel == true) {

			int trainingSize = trainingInstances.size();

			if (trainingSize < Scheduler.updateLearning_MinNumberOfRecords) {

				System.out.println(
						"[small traning dataset for feedback] trainingInstances.size() < Scheduler.updateLearningModelByLastNumberOfRecords: "
								+ trainingInstances.size() + " < " + Scheduler.updateLearning_MinNumberOfRecords);

				return;
			}

			Scheduler.execute_AllBatchQueries(true);

			double accuracy = backend.updateModel(trainingInstances);

			Object[][] backendAccuracy = BlockStorageSimulator.feedbackAccuracy.get(Experiment.clock.intValue());

			int backendIndex = Experiment.backendList.indexOf(backend);

			backendAccuracy[backendIndex][0] = backend;
			backendAccuracy[backendIndex][1] = accuracy;

		} else {
			ArffSaver saver = new ArffSaver();

			saver.setInstances(trainingInstances);

			String saveToPath = "";

			if (path == null || path == "")

				path = Experiment.saveResultPath;

			saveToPath = path + //
					(Scheduler.isTraining == true ? "TRN_" : "EXP_") //
					+ backend.getExperiment().getID() + "_" + backend.getID() + "_" + backend.getDescription()
					+ ".arff";

			saver.setFile(new File(saveToPath));

			// saver.setDestination(new File(path));

			saver.writeBatch();
			// Create the instance

			// edu.purdue.simulation.BlockStorageSimulator.log(Arrays.toString(repTree
			// .distributionForInstance(iExample)));
		}
	}

	/**
	 * @param numberOfRecords
	 *            limits the number of records to be in the resultset coming
	 *            from MySQL DB
	 * @param experiment
	 * @param path
	 *            null will use the default path.
	 * @param includeViolationsNumber
	 *            0: Don't include SLA violations number 1: include SLA
	 *            violations number 2: include SLA violation number and remove
	 *            violation label
	 * @throws Exception
	 */
	public void createUpdateWorkload(int numberOfRecords, Experiment experiment, //
			String path, //
			int includeViolationsNumber, int updateLearningModelByLastNumberOfRecords) throws java.lang.Exception {

		if (updateLearningModelByLastNumberOfRecords > 0)

			includeViolationsNumber = 0; // make it hard constraint

		Connection connection = Database.getConnection();

		// ex_ID, lim, modBy, lastNumOfRecords
		try (CallableStatement cStmt = connection.prepareCall("{call data_for_ML2(?, ?, ?, ?)}")) {

			cStmt.setBigDecimal(1, experiment.getID()); // exp_ID

			cStmt.setInt(2, numberOfRecords); // lim

			if (includeViolationsNumber == 0)

				cStmt.setInt(3, Scheduler.modClockBy); // modby

			else

				cStmt.setInt(3, 0); // modby

			if (Scheduler.feedBackLearning == true)

				cStmt.setInt(4, Scheduler.updateLearning_MaxNumberOfRecords); // lastNumOfRecords

			else

				cStmt.setInt(4, 0); // lastNumOfRecords

			cStmt.execute();

			// ResultSet rs = null;

			boolean hasResultSet = true;

			boolean reportResulSet = true;

			BigDecimal currentBackendID;

			Backend currentBackend = null;

			while (hasResultSet) {

				try (ResultSet rs = cStmt.getResultSet()) {

					if (rs == null)

						break;

					if (reportResulSet) {
						// rs.last() rs.getRow()
						rs.next();

						currentBackendID = rs.getBigDecimal(1);

						for (int i = 0; i < Experiment.backendList.size(); i++) {

							currentBackend = Experiment.backendList.get(i);

							if (currentBackend.getID().compareTo(currentBackendID) == 0)

								break;

							currentBackend = null;
						}

						if (currentBackend == null)

							throw new Exception("could not find the backend in SQL resultset.");

						reportResulSet = false;

					} else {
						// rs.last() rs.getRow()
						this.saveBackend(rs, currentBackend, path, includeViolationsNumber,
								updateLearningModelByLastNumberOfRecords > 0);

						reportResulSet = true;

					}

				}

				hasResultSet = !((cStmt.getMoreResults() == false) && //
						(cStmt.getUpdateCount() == -1));
			}

		}
	}
}
