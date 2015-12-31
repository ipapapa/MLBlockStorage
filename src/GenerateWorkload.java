import java.sql.SQLException;

import edu.purdue.simulation.Workload;

public class GenerateWorkload {
	public static void main(String[] args) {
		
		//DONT USE THIS, just based on probability and posission
		
		Workload workload = new Workload(
				1 // generate method
				,
				"5000 requests | IOPS = { 200, 350, 450 } | potentialVolumeCapacity = { 100, 500, 1000 } | DeleteTime Poisson 600");

		try {
			workload.GenerateWorkload2(120000);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
