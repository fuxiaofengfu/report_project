package report.hive;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import report.jdbc.HiveQuery;
import report.jdbc.MyTransactionalDML;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class HiveToMysql {

	private final static String HIVE_TABLE_DATESTR="-Ddate_year_month_day";

	public static void main(String[] args) throws SQLException {
		insertToMysql(args);
	}

	private static List<Map<String, Object>> getHql(String partitionStr) throws SQLException {
		StringBuilder sql = new StringBuilder();
		sql.append("select count(1) statistics,remote_user,remote_addr, ");
		sql.append("status,http_user_agent ");
		sql.append("from nginx_log ");
		if(StringUtils.isNotEmpty(partitionStr)){
			sql.append("where yearmonth_dir=? and day_dir=? ");
		}
		sql.append("group by remote_user,remote_addr,status,http_user_agent with cube ");

		List<Object> params = null;
		if(StringUtils.isNotEmpty(partitionStr)){
			params = new ArrayList<>();
			String[] split = partitionStr.split("-");
			params.add(""+split[0]+split[1]);
			params.add(String.valueOf(split[2]));
		}
		System.out.println("执行hql-->>>>>"+sql.toString());
		List<Map<String, Object>> query = HiveQuery.query(sql.toString(), params);
		return query;
	}

	/**
	 * 需要参数-Ddate_year_month_day=2017-12-12
	 * @param args
	 * @throws SQLException
	 */
	private static void insertToMysql(String[] args) throws SQLException {
		//解析分区参数
		String dateStr = "";
		for (int i = 0; i < args.length; i++) {
			if(StringUtils.isNotEmpty(args[i]) &&
					args[i].contains(HIVE_TABLE_DATESTR)){
				dateStr = args[i].split("=")[1];
			}
		}
		//这里必须执行分区表,如果没传递分区表分区,则啥都不做
		if(StringUtils.isEmpty(dateStr)){
			return;
		}
		List<Map<String, Object>> hql = getHql(dateStr);
		saveToMysql(hql);
	}

	private static int saveToMysql(List<Map<String, Object>> hiveResult) throws SQLException {

		if(CollectionUtils.isEmpty(hiveResult)){
			return 0;
		}
		StringBuilder builder = new StringBuilder();
		builder.append("insert into nginx_log_report (");
		Map<String, Object> columnMap = hiveResult.get(0);
		int columnIndex = 0;
		for (String column : columnMap.keySet()) {
			builder.append(column);
			columnIndex++;
			if(columnIndex <= columnMap.keySet().size()-1){
				builder.append(",");
			}
		}
		builder.append(",count_time");
		builder.append(") values");

		Date date = new Date();
		for (int i = 0,j=hiveResult.size(); i < j; i++) {//条数
			builder.append("(");
			Map<String,Object> valueMap = hiveResult.get(i);
			columnIndex=0;
			for(Object object : valueMap.values()){
				builder.append(object);
				if(columnIndex<=valueMap.values().size() -2 ){
					builder.append(",");
				}
				columnIndex++;
			}
			builder.append(",").append(date);
			builder.append(")");
			if(i<=j-2){
				builder.append(",");
			}
		}
		System.out.println("执行mysql-->>>>>>"+builder.toString());
		return  MyTransactionalDML.executeDML(builder.toString(), null);
	}
}
