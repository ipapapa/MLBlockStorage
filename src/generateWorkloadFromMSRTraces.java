import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import edu.purdue.simulation.Workload;

public class generateWorkloadFromMSRTraces {
	public static void main(String[] args) throws IOException {

		Workload workload = new Workload(1 // generate method
				,
				"MSR Cambridge Traces | CAMRESWMSA03-lvm0.csv| 100000 requests | IOPS = { 200, 350, 450 } | potentialVolumeCapacity = { 100, 500, 1000 } | DeleteTime Poisson 600");

		try {

			workload.GenerateWorkloadFromMSRCambridgeTraces(100000, // numberOfRequests
					"D:\\Dropbox\\Research\\MLScheduler\\Workload\\MSR Cambridge Traces\\CAMRESWMSA03-lvm0.csv", 50, // deleteFractorPoissionMean
					new int[] { 450, 1000, 2000 }, // potentialVolumeCapacity
					new int[] { 200, 350, 450 }, // potentialIOPS
					100000, // start from
					false // usePoissonRandom
			);

			// test();

		} catch (Exception e) {

			e.printStackTrace();
		}
	}

	public static void test() throws IOException {
		File csvData = new File(
				"D:\\Dropbox\\Research\\MLScheduler\\Workload\\MSR Cambridge Traces\\CAMRESWMSA03-lvm0.csv");

		Charset utf8charset = Charset.forName("UTF-8");

		CSVParser parser = CSVParser.parse(csvData, utf8charset, CSVFormat.RFC4180);

		Date date1 = null;
		Date date2 = null;

		int i = 0;

		long sum = 0;

		for (CSVRecord csvRecord : parser) {

			long time = Long.parseLong(csvRecord.get(0));

			// printTime(csvRecord.get(0));

			if (date1 == null) {

				date1 = printTime(csvRecord.get(0));
			} else {

				date2 = printTime(csvRecord.get(0));

				/*
				 * edu.purdue.simulation.BlockStorageSimulator.log(newDate.
				 * getMillis() - previousDate.getMillis());
				 */

				long diff = getDateDiff(date1, date2, TimeUnit.MILLISECONDS);

				sum += diff;

				edu.purdue.simulation.BlockStorageSimulator.log("delay: " + diff);

				date1 = date2;
			}

			i++;

			if (i == 1000)

				break;
		}

		Long q = sum / i;

		edu.purdue.simulation.BlockStorageSimulator.log("Average diff = " + q.toString());
	}

	public static long getDateDiff(Date date1, Date date2, TimeUnit timeUnit) {
		long diffInMillies = date2.getTime() - date1.getTime();
		return timeUnit.convert(diffInMillies, TimeUnit.MILLISECONDS);
	}

	public static Date printTime(String time) {
		long pwdLastSet = Long.parseLong(time);

		// edu.purdue.simulation.BlockStorageSimulator.log("long value : " +
		// pwdLastSet);

		// Filetime Epoch is JAN 01 1601
		// java date Epoch is January 1, 1970
		// so take the number and subtract java Epoch:
		long javaTime = pwdLastSet - 0x19db1ded53e8000L;

		// convert UNITS from (100 nano-seconds) to (milliseconds)
		javaTime /= 10000;

		// Date(long date)
		// Allocates a Date object and initializes it to represent
		// the specified number of milliseconds since the standard base
		// time known as "the epoch", namely January 1, 1970, 00:00:00 GMT.
		Date theDate = new Date(javaTime);

		// edu.purdue.simulation.BlockStorageSimulator.log("java DATE value : "
		// + theDate);

		SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss.SSS aa zZ");

		// change to GMT time:
		formatter.setTimeZone(TimeZone.getTimeZone("GMT"));

		String newDateString = formatter.format(theDate);

		edu.purdue.simulation.BlockStorageSimulator.log("Date changed format :" + newDateString);

		return theDate;
	}
}
