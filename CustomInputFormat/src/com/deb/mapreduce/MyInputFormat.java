package com.deb.mapreduce;
import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;




public class MyInputFormat extends FileInputFormat<MyKey,MyValue> {
	
	
	@Override
	public RecordReader<MyKey, MyValue> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		return new MyRecordReader();
	}	
}
