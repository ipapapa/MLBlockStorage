package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.State;

public abstract class StochasticEvent {

	protected abstract int[] getSizes();

	protected abstract Random getRandom();

	public abstract int getEventType();

	public abstract void fire(Backend backend) throws SQLException;

	public String toString() {
		return "[StochasticEvent] --> type = " + this.getEventType()
				+ " clock = " + Experiment.clock;
	}

	protected static void apply(StochasticEvent event, Backend backend,
			boolean applyToCapacity) throws SQLException {
		int backendSize = Experiment.BackEndList.size();

		if (backendSize == 0)

			return;

		int randomNumber = event.getRandom().nextInt(
				event.getSizes().length - 0) + 0;

		State backEndState = backend.getState();

		int sumBy = event.getSizes()[randomNumber];

		boolean isEventApplied = false;

		if (applyToCapacity) {

			int newCapacity = backend.getSpecifications().getCapacity() + sumBy;

			if (newCapacity < backEndState.getUsedCapacity()
					|| newCapacity < backend.getSpecifications()
							.getMinCapacity()
					|| newCapacity > backend.getSpecifications()
							.getMaxCapacity()) {
				// can not change the capacity
			} else {
				backend.getSpecifications().setCapacity(newCapacity);

				isEventApplied = true;
			}
		} else { // apply to IOPS

			int newIOPS = backend.getSpecifications().getIOPS() + sumBy;

			if (newIOPS < backend.getSpecifications().getMinIOPS()
					|| newIOPS > backend.getSpecifications().getMaxIOPS()) {

				// cannot change the IOPS

			} else {
				backend.getSpecifications().setIOPS(newIOPS);

				isEventApplied = true;
			}
		}

		if (isEventApplied) {

			event.save(backend.saveCurrentState(), sumBy, null);

			System.out.println("[APPLIED]" + event.toString() + " intVal1 = "
					+ sumBy + " StringVal1 = NULL ");

		} else {

			event.save(backend.getID(), sumBy, null);

			System.out.println("[NOT APPLIED]" + event.toString()
					+ " intVal1 = " + sumBy + " StringVal1 = NULL ");
		}
	}

	protected BigDecimal save(BigDecimal backendID, Integer intVal1,
			String stringVal1) throws SQLException {
		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert into stochastic_event "
								+ "	(stochastic_event_type_ID, backend_ID, clock, int_val1, string_val1)"
								+ "		Values" + "	(?, ?, ? , ?, ?)",
						Statement.RETURN_GENERATED_KEYS);

		statement.setInt(1, this.getEventType());

		statement.setBigDecimal(2, backendID);

		statement.setBigDecimal(3, Experiment.clock);

		statement.setInt(4, intVal1);

		statement.setString(5, stringVal1);

		statement.executeUpdate();

		ResultSet rs = statement.getGeneratedKeys();

		if (rs.next()) {

			return rs.getBigDecimal(1);

		}

		return BigDecimal.valueOf(-1);
	}
}