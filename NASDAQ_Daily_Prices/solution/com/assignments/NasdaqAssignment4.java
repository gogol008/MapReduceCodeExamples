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

public class NasdaqAssignment4 {
	private static DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	    private Text stock_month = new Text();
	    private DoubleWritable price = new DoubleWritable(1.0D);
	    private final static Long threshold=300000L;
	    
	    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
				String line = value.toString();
				String[] tokens = line.split(",");
				if(!tokens[0].equals("exchange"))
				{
					Long volume=Long.parseLong(tokens[7]);
					if(volume>threshold)
					{
						Date date;
						try {
							date = formatter.parse(tokens[2]);
							String monthYear=date.getYear() + "_" + date.getMonth();
							stock_month.set(tokens[1] + "_" + monthYear);
							price.set(Double.parseDouble(tokens[4]));
							output.collect(stock_month, price);
						} catch (ParseException e) {
							e.printStackTrace();
						}
						
					}
				}
	    }
	  }

	  public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
	      double highestPrice = 0;
	      while (values.hasNext()) {
	    	Double price=values.next().get();
	        if(price>highestPrice)
	        {
	        	highestPrice=price;
	        }
	      }
	      output.collect(key, new DoubleWritable(highestPrice));
	    }
	  }

	  public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf(NasdaqAssignment4.class);
	    conf.setJobName("nasdaq5");

	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(DoubleWritable.class);

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