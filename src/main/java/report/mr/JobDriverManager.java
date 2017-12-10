package report.mr;

import org.apache.hadoop.util.ProgramDriver;
import report.mr.jobcontrol.JobControlRun;

public class JobDriverManager {

	public static void main(String[] args) {
		ProgramDriver programDriver = new ProgramDriver();
		try {
			programDriver.addClass("avroToOrc", JobControlRun.class,"avro格式转orc格式输出");
			programDriver.run(args);
		} catch (Throwable throwable) {
			throwable.printStackTrace();
		}
	}
}
