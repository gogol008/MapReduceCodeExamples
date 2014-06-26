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
import org.apache.hadoop.mapred.Reducer; 
import org.apache.hadoop.mapred.Reporter; 
import org.apache.hadoop.mapred.TextInputFormat; 
import org.apache.hadoop.mapred.TextOutputFormat;


/**
* @author droychowdhury 
* @version 1.0
* @since 17-Jan-2014
* @package com.deb.mapreduce
* The Patent program finds the number of sub-patents associated with each id in the provided
* input file. In the map-reduce code the mapper makes key value pair from 
* the input file and reducer does aggregation on this key value pair.
*/


public class Patent {
	
	/** 
	 * @author droychowdhury
	 * @interface Mapper
	 * Map class is static and extends MapReduceBase and implements Mapper 
	 * interface which consists of four hadoop generics type LongWritable, Text, Text, Text.
	 */
 
    public static class patentMap extends MapReduceBase implements 
            Mapper<LongWritable, Text, Text, Text> { 
    	
    	//Mapper
		
		
    	/**
    	 * @method map
    	 * This method takes the input as text data type and splits the input into tokens
    	 * considering whitespace as delimiter. Now key value pair is made and this key 
    	 * value pair is passed to reducer.                                             
    	 * @method_arguments key, value, output, reporter
    	 * @return void
    	 */	
    	
    	//Defining a local variable K of type Text
    	Text k= new Text();

     	//Defining a local variable v of type Text 
    	Text v= new Text(); 

    	/*
    	 * (non-Javadoc)
    	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
    	 */
    	
        @Override 
        public void map(LongWritable key, Text value, 
                OutputCollector<Text, Text> output, Reporter reporter) 
                throws IOException { 

        	//Converting the record (single line) to String and storing it in a String variable line
        	String line = value.toString(); 

         	//StringTokenizer is breaking the record (line) according to the delimiter whitespace
        	StringTokenizer tokenizer = new StringTokenizer(line," "); 
 
         	//Iterating through all the tokens and forming the key value pair	

            while (tokenizer.hasMoreTokens()) { 

            	/* 
            	 * The first token is going in pat, second token in pat1, third token in pat,
            	 * fourth token in pat1 and so on.
            	 */

            	String pat= tokenizer.nextToken();
            	k.set(pat);
            	String pat1= tokenizer.nextToken();
            	v.set(pat1);

            	//Sending to output collector which in turn passes the same to reducer
                output.collect(k,v); 
            } 
        } 
    } 
    
    	//Reducer
	
    /** 
     * @author droychowdhury
     * @interface Reducer
     * Reduce class is static and extends MapReduceBase and implements Reducer 
     * interface having four hadoop generics type Text, Text, Text, IntWritable.
     */
 
    public static class patentReduce extends MapReduceBase implements 
            Reducer<Text, Text, Text, IntWritable> { 
 
    	/**
    	 * @method reduce
    	 * This method takes the input as key and list of values pair from mapper, it does aggregation
    	 * based on keys and produces the final output.                                               
    	 * @method_arguments key, values, output, reporter	
    	 * @return void
    	 */	
    	
    	/*
    	 * (non-Javadoc)
    	 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
    	 */
    	
        @Override 
        public void reduce(Text key, Iterator<Text> values, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException { 

        	//Defining a local variable sum of type int

            int sum = 0; 

            /*
             * Iterates through all the values available with a key and add them together 
             * and give the final result as the key and sum of its values
             */

            while (values.hasNext()) { 
                values.next(); 
                sum ++;
            } 
            
            //Dumping the output
            output.collect(key, new IntWritable(sum)); 
        } 
 
    } 
    
    	//Driver

    /**
     * @method main
     * This method is used for setting all the configuration properties.
     * It is the driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws Exception
     */
 
    public static void main(String[] args) throws Exception { 
 
    	//Creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(Patent.class); 
        conf.setJobName("patent"); 
 
        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class); 
        conf.setOutputValueClass(IntWritable.class); 

        //Setting configuration object with the Data Type of output Key and Value of mapper
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
       
        //Providing the mapper and reducer class names
        conf.setMapperClass(patentMap.class); 
        conf.setReducerClass(patentReduce.class); 

        //Setting format of input and output
         conf.setInputFormat(TextInputFormat.class); 
        conf.setOutputFormat(TextOutputFormat.class); 
 
        //The hdfs input and output directory to be fetched from the command line
        FileInputFormat.setInputPaths(conf, new Path(args[0])); 
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
 
        //Running the job
        JobClient.runJob(conf); 
 
    } 

}
