package org.apache.hadoop.mapreduce.basic;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


	public class MaxTempReduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		
		private IntWritable finalMaxTemperature = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			Integer maxTemp = 0;
			
		System.out.println("!!!!!!!!Input Key to Reducer is " + key.toString());
			
			for(IntWritable value : values) {
			System.out.println("@@@ Iterating value" + value.toString());
				int fetchedValue = value.get();
				if(fetchedValue > maxTemp) {
					maxTemp = fetchedValue;
				}
			}
			finalMaxTemperature.set(maxTemp);
			context.write(key,finalMaxTemperature);
		}
	}