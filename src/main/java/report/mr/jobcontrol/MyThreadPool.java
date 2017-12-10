package report.mr.jobcontrol;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 线程池工具类
 */
public class MyThreadPool {

	/**
	 * 这里线程池里边始终只有一个线程
	 */
	private static ExecutorService executorService= Executors.newFixedThreadPool(3);

	public static void run(Runnable runnable){
		executorService.execute(runnable);
	}

	public static void stop(){
		executorService.shutdownNow();
	}
}
