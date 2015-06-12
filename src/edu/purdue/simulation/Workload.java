package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import edu.purdue.simulation.blockstorage.*;

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

	public void GenerateWorkload() throws SQLException {

		if (super.getID() == null
				|| !(super.getID().compareTo(BigDecimal.ZERO) > 0))

			this.Save();

		this.VolumeRequestList = new ArrayList<VolumeRequest>();

		int[] potentialVolumeCapacity = { 60, 70, 90, 200, 300, 400 };

		Random volumeRandom = new Random();

		int[] potentialIOPS = { 300, 400, 500, 600, 800, 1000 };

		Random IOPSRandom = new Random();

		for (int i = 1; i < 50; i++) {
			// random = getPoissonRandom(10000);

			// Random r = new Random();

			VolumeRequest request = new VolumeRequest(this,
					1, // type
					potentialVolumeCapacity[volumeRandom
							.nextInt(potentialVolumeCapacity.length)],
					potentialIOPS[IOPSRandom.nextInt(potentialIOPS.length)]);

			request.Save();

			this.VolumeRequestList.add(request);
		}
	}

	public void GenerateWorkload2() throws SQLException {

		if (super.getID() == null
				|| !(super.getID().compareTo(BigDecimal.ZERO) > 0))

			this.Save();

		this.VolumeRequestList = new ArrayList<VolumeRequest>();

		int[] potentialVolumeCapacity = { 60, 70, 90, 200, 300, 400 };

		Random volumeRandom = new Random();

		int[] potentialIOPS = { 300, 400, 500, 600, 800, 1000 };

		Random IOPSRandom = new Random();

		int[] gapTimeSecond = { 60, 80, 10, 5, 120, 210 };

		Random gapTimeSecondRandom = new Random();

		for (int i = 1; i < 50; i++) {
			// random = getPoissonRandom(10000);

			// Random r = new Random();

			VolumeRequest request = new VolumeRequest(this, //
					1, // type
					potentialVolumeCapacity[volumeRandom
							.nextInt(potentialVolumeCapacity.length)], //
					potentialIOPS[IOPSRandom.nextInt(potentialIOPS.length)]);

			request.Save();

			try {
				int sleepSeconds = gapTimeSecond[gapTimeSecondRandom
						.nextInt(gapTimeSecond.length)];

				Thread.sleep(sleepSeconds * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			this.VolumeRequestList.add(request);
		}
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
	public boolean Retrieve(BigDecimal ID) throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement("Select	ID, comment, generate_method, create_time"
						+ " From	workload	Where	ID	= ?;");

		statement.setBigDecimal(1, ID);

		ResultSet rs = statement.executeQuery();

		if (rs.next()) {

			this.setGenerateMethod(rs.getInt(3));

			this.setComment(rs.getString(2));

			super.RetrievePersistentProperties(rs, 4);
		}

		return true;
	}

	public boolean RetrieveVolumeRequests() throws SQLException {

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement("Select	ID, workload_ID, capacity, type, IOPS, create_time"
						+ " From	volume_request	Where	workload_ID		= ?;");

		statement.setBigDecimal(1, this.getID());

		ResultSet rs = statement.executeQuery();

		while (rs.next()) {

			VolumeRequest request = new VolumeRequest(this);

			request.RetrieveProperties(rs);

			this.VolumeRequestList.add(request);
		}

		return true;
	}

	@Override
	public String toString() {
		return String.format("ID: %d - comment: %d - GenerateMethod: %d", super
				.getID().toString(), this.Comment, this.GenerateMethod);
	}
}
