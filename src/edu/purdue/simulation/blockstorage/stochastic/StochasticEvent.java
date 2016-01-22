package edu.purdue.simulation.blockstorage.stochastic;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;

import edu.purdue.simulation.Database;
import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.State;

public abstract class StochasticEvent {

	public static boolean saveStochasticEvents = true;

	public static ArrayList<String> queries = new ArrayList<String>();

	protected abstract int[] getSizes();

	protected abstract Random getRandom();

	public abstract int getEventType();

	public abstract void fire(Backend backend) throws SQLException, Exception;

	public String toString(boolean isApplied) {
		return "[StochasticEvent]"
				+ (isApplied ? "[APPLIED]" : "[NOT APPLIED]") + " --> type = "
				+ this.getEventType() + " clock = " + Experiment.clock;
	}

	protected static void apply(StochasticEvent event, Backend backend,
			boolean applyToCapacity) throws SQLException, Exception {

		applyToCapacity = false;// no stochastic event for capacity

		int backendSize = Experiment.backendList.size();

		if (backendSize == 0)

			return;

		// No need for all this

		// int randomNumber = event.getRandom().nextInt(
		// event.getSizes().length - 0) + 0;

		// int sumBy = event.getSizes()[randomNumber];

		State backEndState = backend.getState();

		double possessionMean = backend.getSpecifications()
				.getStabilityPossessionMean();

		// TODO organize this part
		int possessionGeneratedNumber = (int) possessionMean;
		// Scheduler.getPoissonRandom(possessionMean);

		int sumBy = possessionGeneratedNumber * event.getSizes()[0]; // multiply
																		// by
																		// 1 or
																		// -1

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
				// backend.getSpecifications().getIOPS()
				isEventApplied = true;
			}
		}

		if (StochasticEvent.saveStochasticEvents) {
			if (isEventApplied) {

				/*
				 * Save the current available capacity of the backend which
				 * reflects the effect of stochastic events on capacity and
				 * IOPS. It is not needed if recording stochastic events is not
				 * needed
				 */

				backend.saveCurrentState();

				event.save(backend.getID(), sumBy, null, isEventApplied);

			} else {

				event.save(backend.getID(), sumBy, null, isEventApplied);

			}
		}

		edu.purdue.simulation.BlockStorageSimulator.log(event
				.toString(isEventApplied)
				+ " intVal1 = "
				+ sumBy
				+ " StringVal1 = NULL "
				+ " GeneratedNumber = "
				+ possessionGeneratedNumber
				+ " possessionMean = "
				+ possessionMean);
	}

	protected BigDecimal save(BigDecimal backendID, Integer intVal1,
			String stringVal1, boolean isApplied) throws SQLException,
			Exception {

		if (StochasticEvent.saveStochasticEvents == false)

			return BigDecimal.valueOf(-1);

		Connection connection = Database.getConnection();

		PreparedStatement statement = connection
				.prepareStatement(
						"insert into stochastic_event "
								+ "	(stochastic_event_type_ID, backend_ID, clock, int_val1, string_val1, is_applied)"
								+ "		Values" + "	(?, ?, ? , ?, ?, ?)",
						Statement.RETURN_GENERATED_KEYS);

		statement.setInt(1, this.getEventType());

		statement.setBigDecimal(2, backendID);

		statement.setBigDecimal(3, Experiment.clock);

		statement.setInt(4, intVal1);

		statement.setString(5, stringVal1);

		statement.setBoolean(6, isApplied);

		StochasticEvent.queries.add(Database.getQuery(statement));

		// statement.executeUpdate();
		//
		// ResultSet rs = statement.getGeneratedKeys();
		//
		// if (rs.next()) {
		//
		// return rs.getBigDecimal(1);
		//
		// }

		return BigDecimal.valueOf(-1);
	}
}