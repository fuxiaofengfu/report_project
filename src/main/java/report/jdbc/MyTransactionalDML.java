package report.jdbc;

import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
			if(null != connection){
				connection.rollback();
			}
		}finally {
			MyConnection.closeConnect();
		}
		return result;
	}

	/**
	 * //批量执行
	 * @param sql  Map<sql,params>
	 * @return
	 * @throws Exception
	 */
	public static int executeBatchDML(Map<String,List<Object>> sql) throws Exception{
		if(null == sql || sql.isEmpty()){
			return 0;
		}
		int result = 0;
		Connection connection = null;
		try {
			connection = MyConnection.getConnection();
			connection.setAutoCommit(false);
			for(Map.Entry<String,List<Object>> entry : sql.entrySet()){
				String _sql = entry.getKey();
				PreparedStatement preparedStatement = connection.prepareStatement(_sql);
				List<Object> params = entry.getValue();
				if(CollectionUtils.isNotEmpty(params)){
					for (int i = 0; i < params.size(); i++) {
						int pindex = i + 1;
						preparedStatement.setObject(pindex,params.get(i));
					}
				}
				result += preparedStatement.executeUpdate();
			}
			connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
			if(null != connection){
				connection.rollback();
			}
		} finally {
			MyConnection.closeConnect();
		}
		return result;
	}

	public static void main(String[] args) {
		try {
			ArrayList<Object> objects = new ArrayList<>();
			objects.add(1);
			objects.add(2);

			HashMap<String, List<Object>> stringListHashMap = new HashMap<>();
			String sql = "delete from test_sql where id=? or id=?";

			String insertSql = "insert into test_sql values(1),(2)";
			stringListHashMap.put(sql,objects);
			objects = new ArrayList<>();
			stringListHashMap.put(insertSql,objects);

			int result = executeBatchDML(stringListHashMap);
			System.out.println(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
