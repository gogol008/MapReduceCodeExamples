/*For succesfully running this project we need the hadoop common and the mapred jars which should be in the lib directory of the hadoop install
 */

package com.deb.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project the file name is hadoop-core-***.jar.
 */

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
//import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * @author droychowdhury
 * @version 1.0
 * @since 17-Jan-2014
 * @package com.deb.mapreduce The Alphabet program counts the number of times
 *          each word is repeated in an input text file. We write a map reduce
 *          code to achieve this, where mapper makes key value pair from the
 *          input file and reducer does aggregation on this key value pair.
 */

public class wordcount {

	/**
	 * @author droychowdhury
	 * @interface Mapper
	 *            <p>
	 *            Map class is static and extends MapReduceBase and implements
	 *            Mapper interface having four hadoop generics type
	 *            LongWritable, Text, Text, IntWritable
	 */

	public static class wordmapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			String line = value.toString().toLowerCase().replaceAll("\\p{Punct}|\\d", "");
			StringTokenizer tokenizer = new StringTokenizer(line);

			while (tokenizer.hasMoreTokens()) {
				value.set(tokenizer.nextToken());
				output.collect(value, new IntWritable(1));

				// // I am fine I am fine
				// v
				// I 1
				// am 1
				// fine 1
				// I 1
				// am 1
				// fine 1

				// I (1,1)

			}

		}
	}

	/*/ Output types of Mapper should be same as arguments of Partitioner
	public static class MyPartitioner implements Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {

			String myKey = key.toString().toLowerCase();

			if (myKey.equals("spark")) {
				return 0;
			}
			if (myKey.equals("super")) {
				return 1;
			} else {
				return 2;
			}
		}

		@Override
		public void configure(JobConf arg0) {

			// Gives you a new instance of JobConf if you want to change Job
			// Configurations

		}
	} */

	public static class reducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {

			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				// sum = sum + 1;
			}

			// beer,3

			output.collect(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(wordcount.class);
		conf.setJobName("wordcount");

		// Forcing program to run 3 reducers
		//conf.setNumReduceTasks(3);

		conf.setMapperClass(wordmapper.class);
		//conf.setCombinerClass(reducer.class);
		conf.setReducerClass(reducer.class);
		//conf.setPartitionerClass(MyPartitioner.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}
}