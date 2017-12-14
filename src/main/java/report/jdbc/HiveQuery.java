package report.jdbc;

import org.apache.commons.collections.CollectionUtils;
import report.jdbc.hive.HiveConnection;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveQuery {

	public static List<Map<String,Object>> query(String hql,List<Object> params) throws SQLException {
		List<Map<String,Object>> data;
		try {
			Connection hiveConnection = HiveConnection.getHiveConnection();
			PreparedStatement preparedStatement = hiveConnection.prepareStatement(hql);
			if(CollectionUtils.isNotEmpty(params)){
				for (int i = 0; i < params.size(); i++) {
					int pindex = i + 1;
					preparedStatement.setObject(pindex,params.get(i));
				}
			}
			ResultSet resultSet = preparedStatement.executeQuery();
			ResultSetMetaData metaData = resultSet.getMetaData();
			int columnCount = metaData.getColumnCount();
			data = new ArrayList<Map<String,Object>>(100);
			while (resultSet.next()){
				HashMap<String, Object> hashMap = new HashMap<>();
				for (int i = 1; i <= columnCount; i++) {
					hashMap.put(metaData.getColumnName(i),resultSet.getObject(i));
				}
				data.add(hashMap);
			}
		} finally {
			HiveConnection.closeHiveConnect();
		}
		return data;
	}

}
