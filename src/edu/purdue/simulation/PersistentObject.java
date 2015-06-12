package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class PersistentObject {

	public PersistentObject() {

	}

	public PersistentObject(BigDecimal id) throws SQLException {
		this.Retrieve(id);
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

	protected boolean Retrieve(BigDecimal ID) throws SQLException {
		return false;
	}
	
	protected boolean RetrieveProperties(ResultSet resulSet) throws SQLException {
		return false;
	}

	protected void RetrievePersistentProperties(ResultSet resulSet,
			int createTimeIndex) throws SQLException {

		this.setID(resulSet.getBigDecimal(1));

		this.setCreateTime(resulSet.getTimestamp(createTimeIndex));
	}
}
