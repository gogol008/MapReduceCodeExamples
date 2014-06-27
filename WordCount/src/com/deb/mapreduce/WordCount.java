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
* The Alphabet program counts the number of times words of same length have appeared in the input
 * file. We write a map reduce code to achieve this, where mapper makes key value pair from the input 
 * file and reducer does aggregation on this key value pair.
 */

public class WordCount { 
	
	/** 
	 * @author droychowdhury
	 * @interface Mapper
	 * <p>Map class is static and extends MapReduceBase and implements Mapper 
	 * interface having four hadoop generics type LongWritable, Text, IntWritable,
	 * Text
	 */
	
 
    public static class Map extends MapReduceBase implements 
            Mapper<LongWritable, Text, IntWritable,Text> { 

    	
      //Defining a local variable count of type IntWritable       
    	private static IntWritable count ;
 
      //Defining a local variable word of type Text  
         private Text word = new Text(); 
 

     	//Mapper
     	
     	/**
     	 * @method map
     	 * <p>This method takes the input as text data type and splits the input into words.
     	 * Now the length of each word in the input is determined and key value pair is made.
     	 * This key value pair is passed to reducer.                                             
     	 * @method_arguments key, value, output, reporter
     	 * @return void
     	 */	
         
         /*
         * (non-Javadoc)
         * @see org.apache.hadoop.mapred.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
         */
        
        @Override 
        public void map(LongWritable key, Text value, 
                OutputCollector< IntWritable,Text> output, Reporter reporter) 
                throws IOException { 

        	//Converting the record (single line) to String and storing it in a String variable line
            String line = value.toString(); 

            //StringTokenizer is breaking the record (line) into words
            StringTokenizer tokenizer = new StringTokenizer(line); 
 
            //iterating through all the words available in that line and forming the key value pair	
            while (tokenizer.hasMoreTokens()) { 
            	
            	String thisH = tokenizer.nextToken();
            	
            	//finding the length of each token(word)
            	count= new IntWritable(thisH.length()); 
                word.set(thisH); 

                //Sending to output collector which inturn passes the same to reducer
                //So in this case the output from mapper will be the length of a word and that word
                output.collect(count,word); 
            } 
        } 
    } 
 
    //Reducer
	
    /** 
     * @author droychowdhury
     * @interface Reducer
     * <p>Reduce class is static and extends MapReduceBase and implements Reducer 
     * interface having four hadoop generics type IntWritable,Text, IntWritable, IntWritable.
     */
 
    public static class Reduce extends MapReduceBase implements 
            Reducer< IntWritable,Text, IntWritable, IntWritable> { 
 
    	/**
    	 * @method reduce
    	 * <p>This method takes the input as key and list of values pair from mapper, it does aggregation
    	 * based on keys and produces the final output.                                               
    	 * @method_arguments key, values, output, reporter	
    	 * @return void
    	 */	
    	
		 /*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapred.Reducer#reduce(java.lang.Object, java.util.Iterator, org.apache.hadoop.mapred.OutputCollector, org.apache.hadoop.mapred.Reporter)
		 */
    	
        @Override
        public void reduce(IntWritable key, Iterator<Text> values, 
                OutputCollector<IntWritable, IntWritable> output, Reporter reporter) 
                throws IOException { 

        	//Defining a local variable sum of type int
            int sum = 0; 

            /*
             * Iterates through all the values available with a key and add them together and give the final
             * result as the key and sum of its values.
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
     * <p>This method is used for setting all the configuration properties.
     * It acts as a driver for map reduce code.
     * @return void
     * @method_arguments args
     * @throws Exception
     */
    
    public static void main(String[] args) throws Exception { 
 
    	//Creating a JobConf object and assigning a job name for identification purposes
    	JobConf conf = new JobConf(WordCount.class); 
        conf.setJobName("WordCount"); 
 
        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(IntWritable.class); 
        conf.setOutputValueClass(IntWritable.class); 
       
        //Setting configuration object with the Data Type of output Key and Value of mapper
        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setMapOutputValueClass(Text.class);
 
        //Providing the mapper and reducer class names
        conf.setMapperClass(Map.class); 
        conf.setReducerClass(Reduce.class); 
 
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

