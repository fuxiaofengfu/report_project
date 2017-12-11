package report.jdbc;

import report.jdbc.util.PropertyUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MyConnection {

	private static Connection connection;
	static {
		try {
			//加载驱动,尽量使用这种方式避免依赖
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 使用后别忘了关闭链接
	 * @return
	 * @throws Exception
	 * @link closeConnect()
	 */
	public static Connection getConnection() throws Exception {
		String url = PropertyUtil.getProperty("jdbc.url");
		if(null != connection){
			return connection;
		}
		connection = DriverManager.getConnection(url,PropertyUtil.getProperties());
		return connection;
	}

	public static void closeConnect() throws SQLException {
		if(null != connection){
			connection.close();
		}
	}
}
