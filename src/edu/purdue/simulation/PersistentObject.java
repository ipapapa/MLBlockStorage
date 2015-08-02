package edu.purdue.simulation;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class PersistentObject {

	public PersistentObject() {

	}

	public PersistentObject(BigDecimal id) throws SQLException {
		this.retrieve(id);
	}

	private BigDecimal ID;

	private java.sql.Timestamp CreateTime;

	public BigDecimal getID() {
		return ID;
	}

	public void setID(BigDecimal iD) {
		ID = iD;
	}

	public java.sql.Timestamp getCreateTime() {
		return CreateTime;
	}

	public void setCreateTime(java.sql.Timestamp createTime) {
		CreateTime = createTime;
	}

	public BigDecimal save() throws SQLException {
		throw new UnsupportedOperationException("The method is not implimented");
	}

	protected boolean retrieve(BigDecimal ID) throws SQLException {
		throw new UnsupportedOperationException("The method is not implimented");
	}

	protected boolean retrieveProperties(ResultSet resulSet)
			throws SQLException {
		throw new UnsupportedOperationException("The method is not implimented");
	}

	protected void retrievePersistentProperties(ResultSet resulSet,
			int createTimeIndex) throws SQLException {

		this.setID(resulSet.getBigDecimal(1));

		//this.setCreateTime(resulSet.getTimestamp(createTimeIndex));
	}
}
