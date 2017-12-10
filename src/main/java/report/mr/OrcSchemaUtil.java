package report.mr;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;

public class OrcSchemaUtil {

	private static String orc_schema="";

	public static String getOrcSchema(boolean reload) throws IOException {
		if(!reload && StringUtils.isNotEmpty(orc_schema)){
			return orc_schema;
		}
		InputStream resourceAsStream = OrcSchemaUtil.class.getResourceAsStream("/orc_schema.orc");

		if(null == resourceAsStream){
			throw new RuntimeException("orc schema没找到，请确认根目录下存在");
		}
		return IOUtils.toString(resourceAsStream);
	}
}
