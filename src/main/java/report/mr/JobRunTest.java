package report.mr;

import org.apache.hadoop.util.ToolRunner;

public class JobRunTest {


	public static void main(String[] args) throws Exception {

		AvroToOrcJob convertToAvroJob = new AvroToOrcJob();
		int run = ToolRunner.run(convertToAvroJob, args);
		System.exit(run);
	}
}
