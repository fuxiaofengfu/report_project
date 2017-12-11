package report.mr.jobcontrol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import report.mr.AbstractMR;
import report.mr.AvroToOrcJob;
import report.mr.ConvertToAvroJob;

public class JobControlRun {

	private static Logger logger = LoggerFactory.getLogger(JobControlRun.class);

	public static void main(String[] args) throws Exception {

		JobControl jobControl = new JobControl("nginxLogToOrc");
		//设置一个controlledJob开始。。。
		ConvertToAvroJob convertToAvroJob = new ConvertToAvroJob();
		Job avrojob = convertToAvroJob.getJob();
		Configuration configuration1 = avrojob.getConfiguration();
		AbstractMR.handleOutputPath(avrojob);
		ControlledJob avrojobControled = new ControlledJob(configuration1);
		jobControl.addJob(avrojobControled);
		//设置一个controlledJob结束。。。

		//设置一个controlledJob开始。。。
		AvroToOrcJob avroToOrc = new AvroToOrcJob();
		Job avroToOrcJob = avroToOrc.getJob();
		AbstractMR.handleOutputPath(avroToOrcJob);
		Configuration configuration = avroToOrcJob.getConfiguration();
		String outPutPath = configuration1.get(FileOutputFormat.OUTDIR);
		FileInputFormat.setInputPaths(avroToOrcJob,outPutPath);
		ControlledJob controlledJob = new ControlledJob(configuration);
		controlledJob.setJob(avroToOrcJob);
		//添加依赖
		controlledJob.addDependingJob(avrojobControled);
		//设置一个controlledJob结束。。。
		jobControl.addJob(controlledJob);
		//执行结果
		JobControlResult result = JobControlMonitor.monitor(jobControl);
		logger.info("\n任务链执行结果>>>>>>>>>>\n",result);
	}


}
