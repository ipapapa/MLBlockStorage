package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import edu.purdue.simulation.blockstorage.Scheduler;

public class Workload extends PersistentObject {

	public Workload(int generateMethod, String comment) {

		this.GenerateMethod = generateMethod;

		this.Comment = comment;

		this.setVolumeRequestList(new ArrayList<VolumeRequest>());
	}

	public Workload(BigDecimal id) throws SQLException {
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

	public void GenerateWorkload2(int numberOfRequests) throws SQLException {

		if (super.getID() == null
				|| !(super.getID().compareTo(BigDecimal.ZERO) > 0))

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
					potentialVolumeCapacity[volumeRandom
							.nextInt(potentialVolumeCapacity.length)], // Capacity

					potentialIOPS[IOPSRandom.nextInt(potentialIOPS.length)],// IOPS
					Scheduler.getPoissonRandom(600), // Delete Factor
					arrivalTime // Arrival time
			);

			statement.addBatch(request.getSaveQuery());

			if (i % 10000 == 0) {
				statement.executeBatch();

				// statement.close();
			}

			System.out.println("current index: " + i);

			this.VolumeRequestList.add(request);
		}

		System.out.println(this.toString());
	}

	public BigDecimal Save() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection.prepareStatement(
				"insert into workload" + "	(comment, generate_method)"
						+ "		Values" + "	(?, ?);",
				Statement.RETURN_GENERATED_KEYS);

		statement.setString(1, this.Comment);

		statement.setInt(2, this.GenerateMethod);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			super.setID(rs.getBigDecimal(1));

			return super.getID();
		}

		return BigDecimal.valueOf(-1);
	}

	@Override
	public boolean retrieve(BigDecimal ID) throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement("Select	ID, comment, generate_method, create_time"
						+ " From	workload	Where	ID	= ?;");

		statement.setBigDecimal(1, ID);

		ResultSet rs = statement.executeQuery();

		if (rs.next()) {

			this.setGenerateMethod(rs.getInt(3));

			this.setComment(rs.getString(2));

			super.retrievePersistentProperties(rs, 4);
		}

		return true;
	}

	public boolean RetrieveVolumeRequests() throws SQLException {

		return this.RetrieveVolumeRequests(BigDecimal.valueOf(0));
	}

	public boolean RetrieveVolumeRequests(BigDecimal IDBiggerThan)
			throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement("Select	ID, workload_ID, capacity, type, IOPS, Delete_Probability / 1, Arrival_Time / 1" 
						/*
						 * Delete_Probability is deleteFactor
						 */
						+ " From	volume_request	Where	workload_ID		= ?" //
						+ " And	ID	> ?" //
						+ " Limit	" + Scheduler.maxClock + ";");

		statement.setBigDecimal(1, this.getID());

		statement.setBigDecimal(2, IDBiggerThan);

		ResultSet rs = statement.executeQuery();

		while (rs.next()) {

			VolumeRequest request = new VolumeRequest(this);

			request.retrieveProperties(rs);

			this.VolumeRequestList.add(request);
		}

		return true;
	}

	@Override
	public String toString() {
		return String.format("ID: %d - comment: %d - GenerateMethod: %d", super
				.getID().intValue(), this.Comment, this.GenerateMethod);
	}
}
