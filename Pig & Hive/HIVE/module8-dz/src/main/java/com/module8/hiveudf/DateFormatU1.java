package com.module8.hiveudf;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class DateFormatU1 extends UDF {
	
	public Text evaluate(Text dateText) throws IOException {
		DateFormat formatDate=DateFormat.getDateInstance(DateFormat.MEDIUM);
		DateFormat parseFormat=new SimpleDateFormat("yyyy-mm-dd");
		String date=dateText.toString();
		try {
			Date parsedDate=parseFormat.parse(date);
			String formattedDate=formatDate.format(parsedDate);
			return new Text(formattedDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

}
