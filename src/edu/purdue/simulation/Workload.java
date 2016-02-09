package edu.purdue.simulation;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.joda.time.DateTime;
import org.joda.time.Period;

import edu.purdue.simulation.blockstorage.Scheduler;

public class Workload extends PersistentObject {

	public static double devideDeleteFactorBy = 1;

	public Workload(int generateMethod, String comment) {

		this.GenerateMethod = generateMethod;

		this.Comment = comment;

		this.setVolumeRequestList(new ArrayList<VolumeRequest>());
	}

	public Workload(BigDecimal id) throws SQLException, Exception {
		super(id);

		this.setVolumeRequestList(new ArrayList<VolumeRequest>());

	}

	public Workload() {
		this.setVolumeRequestList(new ArrayList<VolumeRequest>());
	}

	private ArrayList<VolumeRequest> VolumeRequestList;

	private String Comment;

	private int GenerateMethod;

	public String getComment() {
		return Comment;
	}

	public void setComment(String comment) {
		Comment = comment;
	}

	public int getGenerateMethod() {
		return GenerateMethod;
	}

	public void setGenerateMethod(int generateMethod) {
		GenerateMethod = generateMethod;
	}

	public ArrayList<VolumeRequest> getVolumeRequestList() {
		return VolumeRequestList;
	}

	public void setVolumeRequestList(ArrayList<VolumeRequest> volumeRequestList) {
		VolumeRequestList = volumeRequestList;
	}

	@SuppressWarnings("deprecation")
	public static long getDateDiff(DateTime date1, DateTime date2, TimeUnit timeUnit) {

		if (timeUnit == TimeUnit.SECONDS) {
			date1 = date1.millisOfSecond().setCopy(0);

			date2 = date2.millisOfSecond().setCopy(0);
		}

		Period period = new Period(date1, date2);

		return period.getSeconds();

		// return

		// long diffInMillies = date2.getTime() - date1.getTime();
		//
		// return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
	}

	public void GenerateWorkloadFromMSRCambridgeTraces(int numberOfRequests, String csvPath,
			int deleteFractorPoissionMean, int[] potentialVolumeCapacity, int[] potentialIOPS, int startFrom)
					throws Exception {

		numberOfRequests += startFrom;

		if (super.getID() == null || !(super.getID().compareTo(BigDecimal.ZERO) > 0))

			this.Save();

		this.VolumeRequestList = new ArrayList<VolumeRequest>();

		Random volumeRandom = new Random();

		Random IOPSRandom = new Random();

		int arrivalTime = 0;

		Connection connection = Database.getConnection();

		Statement statement = connection.createStatement();

		File csvData = new File(csvPath);

		Charset utf8charset = Charset.forName("UTF-8");

		CSVParser parser = CSVParser.parse(csvData, utf8charset, CSVFormat.RFC4180);

		DateTime previousRecordDate = null;

		int i = 0;
		long previousTime = 0;
		int previousArrivalTime = 0;

		for (CSVRecord csvRecord : parser) {

			if (csvRecord.get(3).compareTo("Write") != 0)

				continue;

			long time = Long.parseLong(csvRecord.get(0));

			DateTime recordDate = convertWindowsTime(time);

			if (previousRecordDate == null) {

				previousRecordDate = recordDate;

				edu.purdue.simulation.BlockStorageSimulator.log("Very first record Time :" + recordDate.toString());

			} else {
				i++;

				if (i < startFrom) {

					previousRecordDate = recordDate;

					continue;
				}

				if (i >= numberOfRequests)

					break;

				long diffInMillies = getDateDiff(previousRecordDate, recordDate, TimeUnit.SECONDS);

				if (diffInMillies < 0)

					throw new Exception("Time differerence can not be negative");

				arrivalTime += diffInMillies;// Scheduler.getPoissonRandom(20);

				edu.purdue.simulation.BlockStorageSimulator.log(
						"Time :" + recordDate.toString() + "| dif(seconds)=" + diffInMillies + " | arr=" + arrivalTime);

				// edu.purdue.simulation.BlockStorageSimulator.log("Time :" +
				// formatter.format(recordDate)
				// + "| dif Seconds="
				// + getDateDiff(date1, recordDate, TimeUnit.SECONDS));

				// if (diffInMillies == 0) {
				// edu.purdue.simulation.BlockStorageSimulator.log("diffInMillies
				// == 0 --> arrivalTime = "
				// + arrivalTime);
				// }
				// new DateTime(date1).getMillisOfSecond();
				// new DateTime(recordDate).getMillisOfSecond();
				// if (date1.getTime() == recordDate.getTime()) {
				// edu.purdue.simulation.BlockStorageSimulator.log(i
				// + " - date1.getTime() == recordDate.getTime() ->"
				// + recordDate.toString());
				// }

				previousRecordDate = recordDate;

				// if (time == previousTime) {
				// edu.purdue.simulation.BlockStorageSimulator.log("time ==
				// previousTime --> "
				// + previousTime);
				//
				// Date d1 = convertWindowsTime(time);
				// edu.purdue.simulation.BlockStorageSimulator.log("time --> " +
				// d1);
				// Date d2 = convertWindowsTime(previousTime);
				// edu.purdue.simulation.BlockStorageSimulator.log("time --> " +
				// d2);
				//
				// if (d1.getTime() == d2.getTime()) {
				// edu.purdue.simulation.BlockStorageSimulator.log("d1.getTime()
				// == d2.getTime() ->");
				// }
				//
				// System.out
				// .println("---------------------------------------------");
				// }
				previousTime = time;
				// if (arrivalTime == previousArrivalTime) {
				// edu.purdue.simulation.BlockStorageSimulator.log("arrivalTime
				// == previousArrivalTime ->"
				// + arrivalTime);
				// }
				previousArrivalTime = arrivalTime;

				VolumeRequest request = new VolumeRequest(this, //
						1, // type
						potentialVolumeCapacity[volumeRandom.nextInt(potentialVolumeCapacity.length)], // Capacity

						potentialIOPS[IOPSRandom.nextInt(potentialIOPS.length)], // IOPS
						Scheduler.getPoissonRandom(deleteFractorPoissionMean), // Delete
																				// Factor
						arrivalTime // Arrival time
				);

				statement.addBatch(request.getSaveQuery());

				if (true && i % 5000 == 0) {

					statement.executeBatch();

				}

				// edu.purdue.simulation.BlockStorageSimulator.log("current
				// index: "
				// + i);

				this.VolumeRequestList.add(request);
			}
		}

		statement.executeBatch();

		statement.close();

		edu.purdue.simulation.BlockStorageSimulator.log("workload ID: " + this.getID());
	}

	public static DateTime convertWindowsTime(String time) {
		return convertWindowsTime(Long.parseLong(time));
	}

	public static DateTime convertWindowsTime(long time) {

		// edu.purdue.simulation.BlockStorageSimulator.log("long value : " +
		// pwdLastSet);

		// Filetime Epoch is JAN 01 1601
		// java date Epoch is January 1, 1970
		// so take the number and subtract java Epoch:
		long javaTime = time - 0x19db1ded53e8000L;

		// convert UNITS from (100 nano-seconds) to (milliseconds)
		javaTime /= 10000;

		// Date(long date)
		// Allocates a Date object and initializes it to represent
		// the specified number of milliseconds since the standard base
		// time known as "the epoch", namely January 1, 1970, 00:00:00 GMT.
		DateTime theDate = new DateTime(javaTime);
		// new DateTime(javaTime)
		// edu.purdue.simulation.BlockStorageSimulator.log("java DATE value : "
		// + theDate);

		// SimpleDateFormat formatter = new SimpleDateFormat(
		// "MM/dd/yyyy HH:mm:ss.SSS aa zZ");
		//
		// // change to GMT time:
		// formatter.setTimeZone(TimeZone.getTimeZone("GMT"));
		//
		// String newDateString = formatter.format(theDate);
		//
		// edu.purdue.simulation.BlockStorageSimulator.log("Date changed format
		// :"
		// + newDateString);

		return theDate;
	}

	public void GenerateWorkload2(int numberOfRequests) throws SQLException, Exception {

		if (super.getID() == null || !(super.getID().compareTo(BigDecimal.ZERO) > 0))

			this.Save();

		this.VolumeRequestList = new ArrayList<VolumeRequest>();

		int[] potentialVolumeCapacity = { 100, 500, 1000 };

		Random volumeRandom = new Random();

		int[] potentialIOPS = { 200, 350, 450 };

		Random IOPSRandom = new Random();

		int arrivalTime = 0;

		Connection connection = Database.getConnection();

		Statement statement = connection.createStatement();

		for (int i = 1; i < numberOfRequests; i++) {
			// random = getPoissonRandom(10000);

			// Random r = new Random();

			arrivalTime += Scheduler.getPoissonRandom(20);

			VolumeRequest request = new VolumeRequest(this, //
					1, // type
					potentialVolumeCapacity[volumeRandom.nextInt(potentialVolumeCapacity.length)], // Capacity

					potentialIOPS[IOPSRandom.nextInt(potentialIOPS.length)], // IOPS
					Scheduler.getPoissonRandom(600), // Delete Factor
					arrivalTime // Arrival time
			);

			statement.addBatch(request.getSaveQuery());

			if (i % 10000 == 0) {
				statement.executeBatch();

				// statement.close();
			}

			edu.purdue.simulation.BlockStorageSimulator.log("current index: " + i);

			this.VolumeRequestList.add(request);
		}

		statement.executeBatch();

		statement.close();

		edu.purdue.simulation.BlockStorageSimulator.log(this.toString());
	}

	public BigDecimal Save() throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement(
				"insert into workload" + "	(comment, generate_method)" + "		Values" + "	(?, ?);",
				Statement.RETURN_GENERATED_KEYS)) {

			statement.setString(1, this.Comment);

			statement.setInt(2, this.GenerateMethod);

			statement.executeUpdate();

			try (ResultSet rs = statement.getGeneratedKeys()) {

				if (rs.next()) {

					super.setID(rs.getBigDecimal(1));

					return super.getID();
				}
			}
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public boolean retrieve(BigDecimal ID) throws SQLException, Exception {

		Connection connection = Database.getConnection();

		try (PreparedStatement statement = connection.prepareStatement(
				"Select	ID, comment, generate_method, create_time" + " From	workload	Where	ID	= ?;")) {

			statement.setBigDecimal(1, ID);

			try (ResultSet rs = statement.executeQuery()) {

				if (rs.next()) {

					this.setGenerateMethod(rs.getInt(3));

					this.setComment(rs.getString(2));

					super.retrievePersistentProperties(rs, 4);
				}
			}
		}

		return true;
	}

	public boolean RetrieveVolumeRequests() throws SQLException, Exception {

		return this.RetrieveVolumeRequests(BigDecimal.valueOf(0));
	}

	public boolean RetrieveVolumeRequests(BigDecimal IDBiggerThan) throws SQLException, Exception {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareCall("{call retrieve_workload(?, ?, ?, ?, ?)}");

		statement.setBigDecimal(1, this.getID()); // workload_ID

		statement.setInt(2, Scheduler.maxRequest); // record_limit

		// dont use training dataset for experiment
		if (Scheduler.isTraining)

			statement.setInt(3, 0);

		else
			// dont use training dataset for experiment
			statement.setInt(3, Scheduler.startTestDatasetFromRecordRank); // skip_n_first_records

		statement.setDouble(4, Workload.devideDeleteFactorBy); // devide_delete_by

		statement.setInt(5, Scheduler.modClockBy); // mod_arrival_time

		try (ResultSet rs = statement.executeQuery()) {

			while (rs.next()) {

				VolumeRequest request = new VolumeRequest(this);

				request.retrieveProperties(rs);

				this.VolumeRequestList.add(request);
			}
		}
		
		return true;
	}

	@Override
	public String toString() {
		return String.format("ID: %d - comment: %d - GenerateMethod: %d", super.getID().intValue(), this.Comment,
				this.GenerateMethod);
	}
}
