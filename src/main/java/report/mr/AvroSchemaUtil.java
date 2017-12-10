package report.mr;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.io.InputStream;

public class AvroSchemaUtil {

	private static String AVRO_SCHEMA="";

	public static String getAvroSchemaStr(boolean reload){

		if(!reload && StringUtils.isNotEmpty(AVRO_SCHEMA)){
			return AVRO_SCHEMA;
		}
		InputStream resourceAsStream = AvroSchemaUtil.class.getResourceAsStream("/avro_schema.avsc");
		try {
			AVRO_SCHEMA = IOUtils.toString(resourceAsStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return AVRO_SCHEMA;
	}

	public static Schema  getAvroSchema(boolean reload){
		getAvroSchemaStr(reload);
		if(StringUtils.isEmpty(AVRO_SCHEMA)){
			throw new AvroRuntimeException("avro配置文件不存在，请检查是否在根目录下有配置");
		}
		Schema.Parser parser = new Schema.Parser();
		return parser.parse(AVRO_SCHEMA);
	}
}
