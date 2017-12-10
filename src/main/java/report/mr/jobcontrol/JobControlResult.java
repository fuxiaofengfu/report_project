package report.mr.jobcontrol;

import java.util.Map;

/**
 * 任务链执行结果
 */
public class JobControlResult {

	public static final String SUCCESS = "success";
	public static final String FAIL="fail";

	private String status;
	private Map<String,String> successMap;
	private Map<String,String> failMap;
	private String totalTime;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public Map<String, String> getSuccessMap() {
		return successMap;
	}

	public void setSuccessMap(Map<String, String> successMap) {
		this.successMap = successMap;
	}

	public Map<String, String> getFailMap() {
		return failMap;
	}

	public void setFailMap(Map<String, String> failMap) {
		this.failMap = failMap;
	}

	public String getTotalTime() {
		return totalTime;
	}

	public void setTotalTime(String totalTime) {
		this.totalTime = totalTime;
	}

	@Override
	public String toString() {
		return "\nJobControlResult{\n" +
				"\nstatus='" + status + '\'' +
				", \nsuccessMap=\n" + successMap +
				",\n failMap=\n" + failMap +
				", \ntotalTime='" + totalTime + '\'' +
				'}';
	}
}
