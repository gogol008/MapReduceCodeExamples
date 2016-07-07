package com.assignments;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class USPatentAssignment5 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {
	    private Text year = new Text();
	    private LongWritable ONE = new LongWritable(1);
	    
	    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String[] tokens = line.split(",");
			if(!tokens[0].equals("exchange"))
			{
				year.set(tokens[1]);
				output.collect(year, ONE);
			}
	    }
	  }

	  public static class Reduce extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable> {
	    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, LongWritable> output, Reporter reporter) throws IOException {
	        long count = 0;
	        while (values.hasNext()) {
	          count += values.next().get();
	        }
	        output.collect(key, new LongWritable(count));
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf(USPatentAssignment5.class);
	    conf.setJobName("nasdaq5");

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(LongWritable.class);

	    conf.setMapperClass(Map.class);
	    conf.setCombinerClass(Reduce.class);
	    conf.setReducerClass(Reduce.class);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	    JobClient.runJob(conf);
	  }
}