package report.mr;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class AbstractMR extends Configured implements Tool{

	public final static String FXF_COUNTER = "fuxiaofengCount";
	public final static String FILE_INPUT_PATH= ".file.input.path";
	public final static String FILE_OUT_PATH=".file.output.path";

	protected Logger logger = LoggerFactory.getLogger(AbstractMR.class);

	//0个reduce
	protected static final int ZERO_REDUCE=0;

	public int run(String[] args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		Job runjob = getJob(args);
		boolean b = runjob.waitForCompletion(true);
		long time = stopWatch.getTime();
		stopWatch.stop();
		logger.info("\n*******执行job,jobName={},所花时间为:{}..",runjob.getJobName(),time);
		return b ? 0 : 1;
	}

	public void handleInOutputPath(Job job) throws IOException {

		//处理输入路径
		Configuration configuration = job.getConfiguration();
		String inputPath = getFileInputPathPrefix() + AbstractMR.FILE_INPUT_PATH;
		inputPath=configuration.get(inputPath,"inputpath");
		FileInputFormat.setInputPaths(job,inputPath);
		FileInputFormat.setInputDirRecursive(job,true);
		//处理输出路径
		String outputPath = getFileOutPathPrefix() + AbstractMR.FILE_OUT_PATH;
		outputPath = configuration.get(outputPath,"outputpath");
		FileOutputFormat.setOutputPath(job,new Path(outputPath));
		FileSystem fileSystem = FileSystem.get(configuration);
		if(fileSystem.exists(new Path(outputPath))){
			fileSystem.delete(new Path(outputPath),true);
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
	public abstract Job getJob(String[] args) throws IOException;

	public abstract String getFileInputPathPrefix();
	public abstract String getFileOutPathPrefix();
}