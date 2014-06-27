package com.deb.mapreduce;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class MyValue implements WritableComparable{
	private Text value1,value2;
	
	public MyValue(){
		this.value1 = new Text();
		this.value2 = new Text();
	}
	public MyValue(Text value1,Text value2){
		this.value1 = value1;
		this.value2 = value2;
	}
	public void readFields(DataInput in) throws IOException{
		value1.readFields(in);
		value2.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException{
		value1.write(out);
		value2.write(out);
	}
	public int compareTo(Object o){
		MyValue other = (MyValue)o;
		int cmp = value1.compareTo(other.value1);
		if(cmp != 0){
				return cmp;
		}
		return value2.compareTo(other.value2);
		
	}
	public Text getValue1() {
		return value1;
	}
	public void setValue1(Text value1) {
		this.value1 = value1;
	}
	public Text getValue2() {
		return value2;
	}
	public void setValue2(Text value2) {
		this.value2 = value2;
	}

}
