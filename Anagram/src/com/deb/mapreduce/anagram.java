package com.deb.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class anagram {
	

	public class AnagramMapper extends MapReduceBase implements
    Mapper<LongWritable, Text, Text, Text> {

private Text sortedText = new Text();
private Text orginalText = new Text();


public void map(LongWritable key, Text value,
            OutputCollector<Text, Text> outputCollector, Reporter reporter)
            throws IOException {

    String word = value.toString();
    char[] wordChars = word.toCharArray();
    Arrays.sort(wordChars);
    String sortedWord = new String(wordChars);
    sortedText.set(sortedWord);
    orginalText.set(word);
    outputCollector.collect(sortedText, orginalText);
	}
}

	public class AnagramReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    
    private Text outputKey = new Text();
    private Text outputValue = new Text();

   
    public void reduce(Text anagramKey, Iterator<Text> anagramValues,
                    OutputCollector<Text, Text> results, Reporter reporter) throws IOException {
            String output = "";
            while(anagramValues.hasNext())
            {
                    Text anagam = anagramValues.next();
                    output = output + anagam.toString() + "~";
            }
            StringTokenizer outputTokenizer = new StringTokenizer(output,"~");
            if(outputTokenizer.countTokens()>=2)
            {
                    output = output.replace("~", ",");
                    outputKey.set(anagramKey.toString());
                    outputValue.set(output);
                    results.collect(outputKey, outputValue);
            }
    }
}
	
	public static void main(String[] args) throws Exception{
        JobConf conf = new JobConf(anagram.class);
        conf.setJobName("anagramcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(AnagramMapper.class);
        // conf.setCombinerClass(AnagramReducer.class);
        conf.setReducerClass(AnagramReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

}

}
