package org.apache.hadoop.mapreduce.basic;

import java.io.IOException;
import java.util.Calendar;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMap extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	String mapperInstanceTracker = "mapperInstantiated";

	private IntWritable temperature = new IntWritable();
	private IntWritable dateWhichIsKey = new IntWritable();

	public void map(LongWritable mapperInputKey, Text mapperInputValue,
			Context context) throws IOException, InterruptedException {
		
		System.out.println("~~~~~~~~~~Value of mapperInputKey is "
				+ mapperInputKey);
		System.out.println("Value for mapper instance is "
				+ mapperInstanceTracker);
		
		String line = mapperInputValue.toString();
		System.out.println("********************** Value of line is " + line
				+ ".. at time. " + Calendar.getInstance().getTime());

		this.mapperInstanceTracker = "mapMethodCalled";

		String[] values = line.split(" ");

		String dateKey = values[0];
		String temperatureInStringFormat = values[1];

		int temperatureInIntegerFormat = Integer
				.valueOf(temperatureInStringFormat);
		int dateInIntegerFormat = Integer.valueOf(dateKey);

		dateWhichIsKey.set(dateInIntegerFormat);
		temperature.set(temperatureInIntegerFormat);

		context.write(new Text(dateKey), temperature);
	}
}