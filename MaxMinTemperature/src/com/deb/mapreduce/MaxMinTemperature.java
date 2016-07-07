/*******************************************************************************
   For succesfully running this project we need the hadoop common and the mapred jars which should be in the lib directory of the hadoop install
 ******************************************************************************/

package com.deb.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project the file name is hadoop-core-***.jar.
 */

import org.apache.hadoop.fs.Path;

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

/**
 * @author droychowdhury
 * @version 1.0
 * @since 16-Jan-2014
 * @package com.deb.mapreduce The MaxMinTemperature program finds out the date
 *          when temperature is greater than 40, and calls it as a Hot Day and
 *          when temperature is lower than 10, it calls it as a Cold Day from a
 *          given input file. It also finds all time maximum and minimum
 *          temperature. We write a map reduce code to achieve this, where
 *          mapper makes key value pair from the input file and reducer does
 *          aggregation on this key value pair.
 */

public class MaxMinTemperature {
	// Mapper

	/**
	 * @author droychowdhury
	 * @interface Mapper Map class is static and extends MapReduceBase and
	 *            implements Mapper interface having four hadoop generics type
	 *            LongWritable, Text, Text, Text.
	 */

	public static class TemperatureMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {

		/**
		 * @method map This method takes the input as text data type and and
		 *         tokenizes input by taking whitespace as delimiter. Now
		 *         leaving the first five tokens, 6th token is taken as maxtemp
		 *         and 7th token is taken as mintemp. These maxtemp and mintemp
		 *         are passed to the reducer.
		 * @method_arguments key, value, output, reporter
		 * @return void
		 */

		// Defining a local variable maxtemp of type Text

		Text maxtemp = new Text();

		// Defining a local variable mintemp of type Text

		Text mintemp = new Text();

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object,
		 * java.lang.Object, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */

		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Converting the record (single line) to String and storing it in a
			// String variable line
			String line = value.toString();

			// StringTokenizer is breaking the record (line) according to the
			// delimiter whitespace
			StringTokenizer st = new StringTokenizer(line, " ");

			// Checking for tokens
			while (st.hasMoreTokens()) {
				st.nextToken(); // 1st token
				String date = st.nextToken(); // 2nd token
				st.nextToken(); // 3rd token
				st.nextToken(); // 4th token
				st.nextToken(); // 5th token

				/*
				 * After moving through 1st five fields in the input file, the
				 * 6th field has been taken here as maxtemp and 7th field as
				 * mintemp
				 */
				maxtemp.set(date + "/t" + st.nextToken().trim()); // 6th token
				mintemp.set(date + "/t" + st.nextToken().trim()); // 7th token

				// Both maxtemp and mintemp are collected here
				output.collect(new Text("max"), maxtemp);
				output.collect(new Text("min"), mintemp);

				st.nextToken("/n");

			}
		}
	}

	// Reducer

	/**
	 * @author droychowdhury
	 * @interface Reducer Reduce class is static and extends MapReduceBase and
	 *            implements Reducer interface having four hadoop generics type
	 *            Text, Text, Text, Text.
	 */

	public static class TemperatureReduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {

		/**
		 * @method reduce This method takes the input as key and list of values
		 *         pair from mapper, it does aggregation based on keys and
		 *         produces the final output.
		 * @method_arguments key, values, output, reporter
		 * @return void
		 */

		/*
		 * (non-Javadoc)
		 * 
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object,
		 * java.util.Iterator, org.apache.hadoop.mapred.OutputCollector,
		 * org.apache.hadoop.mapred.Reporter)
		 */

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			// Defining variables maxtemp and mintemp of type float
			float maxtemp = Float.MIN_VALUE;
			float mintemp = Float.MAX_VALUE;

			// Defining variable date of type Text
			Text date = new Text();

			// Converting key to String and dumping it to mykey variable of type
			// String
			String mykey = key.toString();

			// If mykey value matches with "max"
			if (mykey.equals("max"))

			{

				// Checking for values
				while (values.hasNext())

				{

					// Converting the value to string and putting it to variable
					// dateandtemp of type String
					String dateandtemp = values.next().toString();

					// Splitting dateandtemp on delimiter tab "/t" and storing
					// data into dt variable
					String dt[] = dateandtemp.split("/t");

					// Setting the date
					date.set("max tempertature was on :" + dt[0] + "that is");

					// Converting second value in dt to float and storing it to
					// variable temperature of type float
					float temperature = Float.parseFloat(dt[1]);

					// If temperature is greater than maxtemp then maxtemp is
					// assigned that temperature.
					if (maxtemp < temperature)

					{

						maxtemp = temperature;

					}

				}

				// Dumping date and maxtemp
				output.collect(date, new Text(String.valueOf(maxtemp)));

			}

			// If mykey value matches with "min"
			if (mykey.equals("min"))

			{

				// Checking for values
				while (values.hasNext())

				{

					// Converting the value to string and putting it to variable
					// dateandtemp of type String
					String dateandtemp = values.next().toString();

					// Splitting dateandtemp on delimiter tab "/t" and storing
					// data into dt variable
					String dt[] = dateandtemp.split("/t");

					// Setting the date
					date.set("min tempertature was on :" + dt[0] + "that is");

					// Converting second value in dt to float and storing it to
					// variable temperature of type float
					float temperature = Float.parseFloat(dt[1]);

					// If temperature is greater than mintemp then mintemp is
					// assigned that temperature.
					if (mintemp > temperature)

					{

						mintemp = temperature;

					}

				}

				// Dumping date and mintemp
				output.collect(date, new Text(String.valueOf(mintemp)));

			}

		}

	}

	// Driver

	/**
	 * @method main This method is used for setting all the configuration
	 *         properties. It acts as a driver for map reduce code.
	 * @return void
	 * @method_arguments args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception

	{

		// Creating a JobConf object and assigning a job name for identification
		// purposes
		JobConf conf = new JobConf(MaxMinTemperature.class);
		conf.setJobName("hotcolddays");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		// Setting configuration object with the Data Type of output Key and
		// Value of mapper
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		// Providing the mapper and reducer class names
		conf.setMapperClass(TemperatureMap.class);
		conf.setReducerClass(TemperatureReduce.class);

		// Setting format of input and output
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		// The hdfs input and output directory to be fetched from the command
		// line
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		// Running the job
		JobClient.runJob(conf);

	}
}
