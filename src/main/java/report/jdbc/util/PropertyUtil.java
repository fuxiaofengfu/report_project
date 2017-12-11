package report.jdbc.util;

import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.util.Properties;

/**
 * 配置文件工具
 * 缓存配置文件
 */
public class PropertyUtil {

	public static String getProperty(String property){
		return properties.getProperty(property);
	}

	public static String getProperty(String property,String defaultValue){
		if(StringUtils.isEmpty(property)){
			return defaultValue;
		}
		return properties.getProperty(property,defaultValue);
	}

	public static Properties getProperties(){
		return properties;
	}

	private static Properties properties;
	static {
		try {
			loadProperties();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void loadProperties() throws IOException {

		if(null != properties && !properties.isEmpty()){
			return;
		}
		properties = new Properties();
		properties.load(PropertyUtil.class.getResourceAsStream("/resources.properties"));
	}
}
