package report.mr;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractMR extends Configured implements Tool{

	protected Logger logger = LoggerFactory.getLogger(AbstractMR.class);

	//0个reduce
	protected static final int ZERO_REDUCE=0;

	public int run(String[] args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		Job runjob = getJob();
		handleOutputPath(runjob);
		boolean b = runjob.waitForCompletion(true);
		long time = stopWatch.getTime();
		stopWatch.stop();
		logger.info("\n*******执行job,jobName={},所花时间为:{}..",runjob.getJobName(),time);
		return b ? 0 : 1;
	}

	public static void handleOutputPath(Job runjob) throws IOException {
		Configuration configuration = runjob.getConfiguration();
		String outPutPath = configuration.get(FileOutputFormat.OUTDIR);
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(new Path(outPutPath))){
			fileSystem.delete(new Path(outPutPath),true);
		}
	}

	public static String timeFormat(long time){
		time = time / 1000;//忽略毫秒值
		long hour = time / 60 / 60;
		long minute = time % (60 * 60)/60;
		long seconds = time % 60;
		return hour + "小时"+minute+"分钟"+seconds+"秒";
	}
	/**
	 * 设置jobName
	 * @return
	 */
	public abstract String getJobName();

	/**
	 * 获取job
	 * @return
	 */
	public abstract Job getJob() throws IOException;
}