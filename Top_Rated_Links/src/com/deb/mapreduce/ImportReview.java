package com.deb.mapreduce;

import java.io.IOException;
import java.util.List;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;


public class ImportReview {

	static String DELIMITER = ",";

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

		// 1, "one tow three" -> one, 1 and two ,1 and three,1
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) 
				throws IOException {

			XMLParser xP;
			try {
				xP = new XMLParser(value.toString());

				List<String> categories = xP.getCategory();
				List<String> reviewLists = xP.getReviews();
				int postive = 0;
				int negative = 0;

				for (String eachReview : reviewLists) {
					if (BadWords.isBad(eachReview)) {
						negative++;
					} else {
						postive++;
					}
				}

				for (String eachCat : categories) {
					String mapOutput = WordUtil.cleanWords(eachCat) + DELIMITER + xP.getHash() + DELIMITER + xP.getUrl() + DELIMITER + postive + DELIMITER + negative + DELIMITER + xP.getUsercount();
					System.out.println(" output " + mapOutput);
					output.collect(new Text(""), new Text(mapOutput));
				}
			} catch (Exception e) {
				e.printStackTrace();
				throw new RuntimeException("");
			}


		}

	}

	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(ImportReview.class);
		//conf.set("fs.default.name", "hdfs://localhost:54310");
		conf.set(XmlInputFormat.START_TAG_KEY, "<document>");
		conf.set(XmlInputFormat.END_TAG_KEY, "</document>");
		conf.setJobName("importreview");
		conf.setNumReduceTasks(0);
		conf.setMapperClass(Map.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// how the data will be read
		conf.setInputFormat(XmlInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);

	}
}
