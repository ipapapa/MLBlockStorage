import java.math.BigDecimal;
import java.util.ArrayList;

import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.Scheduler;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.LVM;

public class generateTrainingDateset {
	public static void main(String[] args) {
		Experiment ex = new Experiment(null, null, null);

		int[] IDs = new int[7];

		// IDs[0] = 616;
		// IDs[1] = 427327;

		IDs[0] = 761;
		IDs[1] = 428072;

		for (int i = 2; i < 7; i++) {
			IDs[i] = IDs[1] + i - 1;
		}

		ex.setID(new BigDecimal(IDs[0]));

		Backend b1 = new LVM(ex, null, null);
		b1.setID(new BigDecimal(IDs[1]));
		Experiment.backendList.add(b1);

		Backend b2 = new LVM(ex, null, null);
		b2.setID(new BigDecimal(IDs[2]));
		Experiment.backendList.add(b2);

		Backend b3 = new LVM(ex, null, null);
		b3.setID(new BigDecimal(IDs[3]));
		Experiment.backendList.add(b3);

		Backend b4 = new LVM(ex, null, null);
		b4.setID(new BigDecimal(IDs[4]));
		Experiment.backendList.add(b4);

		Backend b5 = new LVM(ex, null, null);
		b5.setID(new BigDecimal(IDs[5]));
		Experiment.backendList.add(b5);

		Backend b6 = new LVM(ex, null, null);
		b6.setID(new BigDecimal(IDs[6]));
		Experiment.backendList.add(b6);

		try {

			/*
			 * This API is changed, fix it if u need this program
			 */

			ex.createTrainingDataForRepTree(
			//
					0,//
					ex,//
						// null
					String.format(
							"D:\\Dropbox\\Research\\experiment\\%d\\",
							IDs[0]), //
					0 // dont iunclude SLA violation numbers
					);

			System.out.println("DONE!");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
