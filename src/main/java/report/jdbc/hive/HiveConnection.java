package report.jdbc.hive;

import report.jdbc.util.PropertyUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class HiveConnection {

    private static Connection hiveConnection;

	public static Connection getHiveConnection() throws SQLException {
		String hiveUrl = PropertyUtil.getProperty("hive.server2.url");
		if(null != hiveConnection){
			return hiveConnection;
		}
		String hiveUser = PropertyUtil.getProperty("hive.db.user");
		String hivepwd = PropertyUtil.getProperty("hive.db.password");
		hiveConnection = DriverManager.getConnection(hiveUrl,hiveUser,hivepwd);
		return hiveConnection;
	}

	public static void closeHiveConnect() throws SQLException {
		if(null != hiveConnection){
			hiveConnection.close();
		}
	}

	static {
		try {
			//加载驱动,尽量使用这种方式避免依赖
			Class.forName("org.apache.hive.jdbc.HiveDriver").newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
