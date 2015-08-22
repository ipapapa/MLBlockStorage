import java.math.BigDecimal;
import java.util.ArrayList;

import edu.purdue.simulation.Experiment;
import edu.purdue.simulation.blockstorage.backend.Backend;
import edu.purdue.simulation.blockstorage.backend.LVM;

public class generateTrainingDateset {
	public static void main(String[] args) {
		Experiment ex = new Experiment(null, null, null);

		ex.setID(new BigDecimal(594));

		Backend b1 = new LVM(null, null, null);
		b1.setID(new BigDecimal(186752));
		Experiment.backendList.add(b1);

		Backend b2 = new LVM(null, null, null);
		b2.setID(new BigDecimal(186753));
		Experiment.backendList.add(b2);

		Backend b3 = new LVM(null, null, null);
		b3.setID(new BigDecimal(186754));
		Experiment.backendList.add(b3);

		Backend b4 = new LVM(null, null, null);
		b4.setID(new BigDecimal(186755));
		Experiment.backendList.add(b4);

		Backend b5 = new LVM(null, null, null);
		b5.setID(new BigDecimal(186756));
		Experiment.backendList.add(b5);

		Backend b6 = new LVM(null, null, null);
		b6.setID(new BigDecimal(186757));
		Experiment.backendList.add(b6);

		for (int i = 0; i < Experiment.backendList.size(); i++) {

			Backend backend = Experiment.backendList.get(i);

			try {
				Backend.createTrainingDataForRepTree( //
						0,//
						ex,//
						backend,//
						String.format("D:\\Research\\experiment\\Custom\\backend%d_%s_ex%s.arff", //
								i + 1, //
								backend.getID().toString(), //
								ex.getID().toString()));

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}
}
