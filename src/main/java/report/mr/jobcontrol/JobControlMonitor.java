package report.mr.jobcontrol;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import report.mr.AbstractMR;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;


/**
 * 任务链执行监视器
 */
public class JobControlMonitor {

	private Logger logger = LoggerFactory.getLogger(JobControlMonitor.class);

	private static JobControl mjobControl;

	public static JobControlResult monitor(JobControl jobControl) throws Exception {

		JobControl.ThreadState threadState = jobControl.getThreadState();
		if(!JobControl.ThreadState.READY.equals(threadState)){
			throw new RuntimeException("当前jobControl状态不对,请检查健康状态");
		}
		mjobControl = jobControl;
		//获取任务结果
		Callable<JobControlResult> callable = new Callable<JobControlResult>(){
			JobControlResult jobControlResult = new JobControlResult();
			@Override
			public JobControlResult call() throws Exception {
				long beginTime = System.currentTimeMillis();
				MyThreadPool.run(mjobControl);
				while(!mjobControl.allFinished()){
					TimeUnit.SECONDS.sleep(3);
				}
				mjobControl.stop();
				long endTime = System.currentTimeMillis();
				List<ControlledJob> failedJobList = mjobControl.getFailedJobList();
				List<ControlledJob> successfulJobList = mjobControl.getSuccessfulJobList();
				Map<String,String> fail = new HashMap<>();
				if(CollectionUtils.isNotEmpty(failedJobList)){
					jobControlResult.setStatus(JobControlResult.FAIL);
					for(ControlledJob job : failedJobList){
						Job job1 = job.getJob();
						fail.put("\n"+job1.getJobName()+":"+job1.getJobID(),getJobInfo(job1));
					}
				}else{
					jobControlResult.setStatus(JobControlResult.SUCCESS);
				}
				Map<String,String> success = new HashMap<>();
				if(CollectionUtils.isNotEmpty(successfulJobList)){
					for(ControlledJob job : successfulJobList){
						Job job1 = job.getJob();
						success.put("\n"+job1.getJobName()+":"+job1.getJobID(),getJobInfo(job1));
					}
				}
				jobControlResult.setFailMap(fail);
				jobControlResult.setSuccessMap(success);
				jobControlResult.setTotalTime(AbstractMR.timeFormat(endTime-beginTime));
				return jobControlResult;
			}
		};
		FutureTask<JobControlResult> future = new FutureTask<>(callable);
		JobControlResult jobControlResult;
		try{
			MyThreadPool.run(future);
			jobControlResult = future.get();
		}finally {
			//线程池关掉
			MyThreadPool.stop();
		}
		return  jobControlResult;
	}

	private static String getJobInfo(Job job) throws IOException, InterruptedException {
		if(null == job){
			return null;
		}
		StringBuilder builder = new StringBuilder();
		long time = job.getFinishTime() - job.getStartTime();
		String timeStr = AbstractMR.timeFormat(time);
		Counters counters = job.getCounters();
		Iterator<CounterGroup> iterator = counters.iterator();
		while(iterator.hasNext()){
			CounterGroup next = iterator.next();
			builder.append("\ngroupName=").append(next.getDisplayName());
			Iterator<Counter> iterator1 = next.iterator();
			while(iterator1.hasNext()) {
				Counter next1 = iterator1.next();
				builder.append("\n\t\t");
				builder.append("counterName=").append(next1.getDisplayName())
						.append(",counerValue=").append(next1.getValue());
			}
		}
		builder.append("\njob").append(job.getJobName()).append("所花时间:").append(timeStr);
		return builder.toString();
	}
}