package report.mr;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.List;

public class ConvertToAvroJob extends AbstractMR{
	/**
	 * 设置jobName
	 *
	 * @return
	 */
	@Override
	public String getJobName() {
		return "convertToAvro";
	}

	/**
	 * 获取job
	 *
	 * @return
	 */
	@Override
	public Job getJob() throws IOException {

		Configuration conf = super.getConf();
		if(null == conf){
			conf = new Configuration();
		}
		Job job = Job.getInstance(conf,this.getJobName());
		job.setJarByClass(ConvertToAvroJob.class);
		job.setNumReduceTasks(ZERO_REDUCE);
		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setMapperClass(AvroMapper.class);
		FileInputFormat.setInputPaths(job,"input");
		AvroKeyOutputFormat.setOutputPath(job,new Path("avro_input"));
		AvroJob.setOutputKeySchema(job,AvroSchemaUtil.getAvroSchema(false));
		return job;
	}

	private static class AvroMapper extends Mapper<LongWritable,Text,AvroKey<GenericData.Record>,NullWritable>{

		Schema schema = AvroSchemaUtil.getAvroSchema(false);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] valueArr = value.toString().split("\001");
			GenericData genericData = GenericData.get();
			GenericData.Record data = (GenericData.Record)genericData.newRecord(null, schema);
			AvroKey<GenericData.Record> objectAvroKey = new AvroKey<GenericData.Record>();
			List<Schema.Field> fields = schema.getFields();
			for(int i=0;i<fields.size();i++){
				Schema.Field field = fields.get(i);
				String avroValue = valueArr[i];
				if(StringUtils.isNotEmpty(avroValue) && "-".equals(avroValue)){
					avroValue = "";
				}
				data.put(field.name(),avroValue);
			}
			objectAvroKey.datum(data);
			context.write(objectAvroKey,NullWritable.get());
		}
	}
}
