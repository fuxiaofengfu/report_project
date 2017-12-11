package report.mr;

import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapreduce.OrcOutputFormat;

import java.io.IOException;
import java.util.List;

public class AvroToOrcJob extends AbstractMR {

	public final static String FILE_INPUT_PATH_PREFIX="orc";
	public final static String FILE_OUTPUT_PATH_PREFIX="orc";

	@Override
	public String getJobName() {
		return "avroToOrcJob";
	}

	/**
	 * 获取job
	 *
	 * @return
	 */
	@Override
	public Job getJob(String[] args) throws IOException {

		Configuration conf = super.getConf();
		if(null == conf){
			conf = new Configuration();
		}
		//解析-D参数
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		args = parser.getRemainingArgs();
		OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf,OrcSchemaUtil.getOrcSchema(false));
		Job job = Job.getInstance(conf,this.getJobName());
		job.setJarByClass(AvroToOrcJob.class);
		job.setNumReduceTasks(ZERO_REDUCE);
		job.setMapperClass(AvroMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(OrcStruct.class);
		job.setInputFormatClass(AvroKeyInputFormat.class);
		AvroJob.setInputKeySchema(job,AvroSchemaUtil.getAvroSchema(false));

		job.setOutputFormatClass(OrcOutputFormat.class);
		handleInOutputPath(job);
		return job;
	}

	@Override
	public String getFileInputPathPrefix() {
		return FILE_INPUT_PATH_PREFIX;
	}

	@Override
	public String getFileOutPathPrefix() {
		return FILE_OUTPUT_PATH_PREFIX;
	}

	private static class AvroMapper extends Mapper<AvroKey<GenericData.Record>,NullWritable,NullWritable,OrcStruct>{
		private TypeDescription schema =
				TypeDescription.fromString(OrcSchemaUtil.getOrcSchema(false));
		private OrcStruct pair = (OrcStruct)OrcStruct.createValue(schema);

		private AvroMapper() throws IOException {
		}

		@Override
		protected void map(AvroKey<GenericData.Record> key, NullWritable value, Context context) throws IOException, InterruptedException {
			GenericData.Record datum = key.datum();
			List<String> fieldNames = schema.getFieldNames();
			for (String filed : fieldNames) {
				String outV = (String) datum.get(filed);
				Text out = new Text();
				out.set(outV);
				pair.setFieldValue(filed, out);
			}
			context.write(NullWritable.get(), pair);
		}
	}


}
