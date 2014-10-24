package com.deb.mapreduce;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;





public class ImageDriver extends Configured implements Tool
{
	
    public int run(String[] args) throws Exception
    {
          //getting configuration object and setting job name
          Configuration conf = getConf();
      Job job = new Job(conf, "Word Count hadoop-0.20");
    
      //setting the class names
      job.setJarByClass(ImageDriver.class);
      job.setMapperClass(ImageDuplicatesMapper.class);
      job.setInputFormatClass(SequenceFileInputFormat.class);
      //job.setCombinerClass(WordCountReducer.class);
      job.setReducerClass(ImageDupsReducer.class);
      
      
      //setting the output data type classes
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      //to accept the hdfs input and outpur dir at run time
      FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

      return job.waitForCompletion(true) ? 0 : 1;
  }
    
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ImageDriver(), args);
        System.exit(res);
    }
	}