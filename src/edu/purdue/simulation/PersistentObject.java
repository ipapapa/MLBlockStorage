package edu.purdue.simulation;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class PersistentObject {

	public PersistentObject() {

	}

	public PersistentObject(BigDecimal id) throws SQLException, Exception {
		this.retrieve(id);
	}

	private BigDecimal ID;

	private java.sql.Timestamp CreateTime;

	public BigDecimal getID() {
		return this.ID;
	}

	public void setID(BigDecimal iD) {
		this.ID = iD;
	}

	public java.sql.Timestamp getCreateTime() {
		return this.CreateTime;
	}

	public void setCreateTime(java.sql.Timestamp createTime) {
		this.CreateTime = createTime;
	}

	public BigDecimal save() throws SQLException, Exception {
		throw new UnsupportedOperationException("The method is not implimented");
	}

	protected boolean retrieve(BigDecimal ID) throws SQLException, Exception {
		throw new UnsupportedOperationException("The method is not implimented");
	}

	protected boolean retrieveProperties(ResultSet resulSet)
			throws SQLException, Exception {
		throw new UnsupportedOperationException("The method is not implimented");
	}

	protected void retrievePersistentProperties(ResultSet resulSet,
			int createTimeIndex) throws SQLException, Exception {

		this.setID(resulSet.getBigDecimal(1));

		// this.setCreateTime(resulSet.getTimestamp(createTimeIndex));
	}
}
