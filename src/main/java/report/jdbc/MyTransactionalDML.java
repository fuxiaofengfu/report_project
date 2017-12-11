package report.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * insert update delete
 */
public class MyTransactionalDML {

	/**
	 * 简单connection级别的事务
	 * @param sql
	 * @param params
	 * @return
	 * @throws SQLException
	 */
	public static int executeDML(String sql,List<Object> params) throws SQLException {
		int result = 0;
		Connection connection = null;
		try {
			connection = MyConnection.getConnection();
			connection.setAutoCommit(false);
			PreparedStatement preparedStatement = connection.prepareStatement(sql);
			if(null != params && !params.isEmpty()){
				for (int i = 0; i < params.size(); i++) {
					int pindex = i + 1;
					preparedStatement.setObject(pindex,params.get(i));
				}
			}
			result = preparedStatement.executeUpdate();
			connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
			connection.rollback();
		}finally {
			MyConnection.closeConnect();
		}
		return result;
	}

	public static void main(String[] args) {
		try {
			ArrayList<Object> objects = new ArrayList<>();
			objects.add(1);
			objects.add(2);
			int result = executeDML("delete from test_sql where id=? or id=?",objects);
			System.out.println(result);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
