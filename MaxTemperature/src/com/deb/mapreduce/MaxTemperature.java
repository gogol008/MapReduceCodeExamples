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
* @since 16-Jan-2014
* @package com.deb.mapreduce
* The Temperature program finds out the maximum temperature in every year from the given input file. 
* The output data is in the form of “Year and Maximum temperature corresponding to that year".
* We write a map reduce code to achieve this, where mapper makes key value pair from the input file
* and reducer does aggregation on this key value pair. 
*/

public class MaxTemperature {
	/**
 	 * @interface Mapper
	 * Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, Text, 
	 * IntWritable.
	 */
 
    public static class MaxTempMap extends MapReduceBase implements 
            Mapper<LongWritable, Text, Text, IntWritable> { 

    	//Mapper
		
		
    	/**
    	 * @method map
    	 * This method takes the input as text data type and and splits the input into tokens
    	 * by taking whitespace as delimiter. The first token goes year and second token is temperature,
    	 * this is repeated till last token. Now key value pair is made and passed to reducer.                                             
    	 * @method_arguments key, value, output, reporter
    	 * @return void
    	 */	
    	
    	//Defining a local variable k of type Text  
    	Text k= new Text(); 

    	/*
    	 * (non-Javadoc)
    	 * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
    	 */
    	
        @Override 
        public void map(LongWritable key, Text value, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) 
                throws IOException { 

        		//Converting the record (single line) to String and storing it in a String variable line
                String line = value.toString(); 

                //StringTokenizer is breaking the record (line) according to the delimiter whitespace
                StringTokenizer tokenizer = new StringTokenizer(line," "); 
 
                //Iterating through all the tokens and forming the key value pair
                while (tokenizer.hasMoreTokens()) { 

                //The first token is going in year variable of type string
            	String year= tokenizer.nextToken();
            	k.set(year);

            	//Takes next token and removes all the white spaces around it and then stores it in the string variable called temp
            	String temp= tokenizer.nextToken().trim();

            	//Converts string temp into integer v       	
            	int v = Integer.parseInt(temp); 

            	//Sending to output collector which in turn passes the same to reducer
                output.collect(k,new IntWritable(v)); 
            } 
        } 
    } 
 
    //Reducer
	
    /** 
     * @interface Reducer
     * Reduce class is static and extends MapReduceBase and implements Reducer 
     * interface having four hadoop generics type Text, IntWritable, Text, IntWritable.
     */
    
    public static class MaxTempReduce extends MapReduceBase implements 
            Reducer<Text,IntWritable, Text, IntWritable> { 

    	/**
    	 * @method reduce
    	 * This method takes the input as key and list of values pair from mapper, it does aggregation
    	 * based on keys and produces the final output.                                               
    	 * @method_arguments key, values, output, reporter	
    	 * @return void
    	 */	
    	 
    	//Defining a local variable maxtemp of type int
    	 int maxtemp=0;

    	 /*
    	  * (non-Javadoc)
    	  * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
    	  */
    	 
        @Override 
        public void reduce(Text key, Iterator<IntWritable> values, 
                OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
           
        	/*
        	 * Iterates through all the values available with a key and if the integer variable temperature
        	 * is greater than maxtemp, then it becomes the maxtemp
        	 */

            while (values.hasNext()) { 

            //Defining a local variable temperature of type int which is taking all the temperature
            int temperature= values.next().get(); 
            
            	if(maxtemp<temperature)
            	{
            		maxtemp =temperature;
            	}
            } 
            
            //Finally the output is collected as the year and maximum temperature corresponding to that year
            output.collect(key, new IntWritable(maxtemp)); 
        } 
 
    } 
    
  //Driver

    /**
     * @method main
     * <p>This method is used for setting all the configuration properties.
     * It acts as a driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws Exception
     */
 
    public static void main(String[] args) throws Exception { 
 
        //Creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(MaxTemperature.class); 
        conf.setJobName("maxtemp"); 
 
        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class); 
        conf.setOutputValueClass(IntWritable.class); 
 
        //Setting configuration object with the Data Type of output Key and Value of mapper
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
       
        //Providing the mapper and reducer class names
        conf.setMapperClass(MaxTempMap.class); 
        conf.setReducerClass(MaxTempReduce.class); 
 
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
