package FirstProject.Join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DeptMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String line=value.toString();
		String[] arr=line.split(",");
		String deptId=arr[0];
		String name=arr[1];
		context.write(new IntWritable(Integer.parseInt(deptId)), new Text(name));
	}
}
