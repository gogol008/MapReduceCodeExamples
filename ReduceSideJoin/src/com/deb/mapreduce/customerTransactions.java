package com.deb.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class customerTransactions {
	public static class CustsMapper extends
	Mapper<Object, Text, Text, Text> {
public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[0]), new Text("custs\t" + parts[1]));
}
}

public static class TxnsMapper extends
	Mapper<Object, Text, Text, Text> {
public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	String record = value.toString();
	String[] parts = record.split(",");
	context.write(new Text(parts[2]), new Text("txns\t" + parts[3]));
}
}

public static class ReduceJoinReducer extends
	Reducer<Text, Text, Text, Text> {
public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	String name = "";
	double total = 0.0;
	int count = 0;
	for (Text t : values) {
		String parts[] = t.toString().split("\t");
		if (parts[0].equals("txns")) {
			count++;
			total += Float.parseFloat(parts[1]);
		} else if (parts[0].equals("custs")) {
			name = parts[1];
		}
	}
	String str = String.format("%d\t%f", count, total);
	context.write(new Text(name), new Text(str));
}
}

@SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = new Job(conf, "Reduce-side join");
job.setJarByClass(customerTransactions.class);
job.setReducerClass(ReduceJoinReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);


MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
Path outputPath = new Path(args[2]);


FileOutputFormat.setOutputPath(job, outputPath);
outputPath.getFileSystem(conf).delete(outputPath);
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
