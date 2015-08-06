/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce.basic;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTempTR extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		System.out
				.println("************************** In main method ****************");

		Configuration configuration = new Configuration();
		MaxTempTR dateTR = new MaxTempTR();

		int exitCode = ToolRunner.run(configuration, dateTR, args);
		System.exit(exitCode);
	}

	public int run(String[] arg0) throws Exception {

		Configuration conf = getConf();
		if (arg0.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf,
				"Tool Runner Max Temperature for Station and Date "
						+ Calendar.getInstance().getTime());
		int numberOfReduceTasks = 1;
		int linesPerMapper = 1;
		for (Entry<String, String> entry : conf) {
			
//			if(entry.getKey().contains("mapreduce.")) {
//				System.out.println("Key is "+entry.getKey()
//						+" and value is "+entry.getValue());
//			}
			

			if (entry.getKey().equalsIgnoreCase("reducerTaskHandler")) {
				System.out.println("Value for reducer parameter is "
						+ Integer.valueOf(entry.getValue()));
				numberOfReduceTasks = Integer.valueOf(entry.getValue());
			}
			String currentKeyUnderIteration = entry.getKey();
			if (currentKeyUnderIteration.equalsIgnoreCase("mapperTaskHandler")) {
				System.out.println("Value for passed parameter is "
						+ Integer.valueOf(entry.getValue()));
				linesPerMapper = Integer.valueOf(entry.getValue());
			}
			// job.getConfiguration().setInt(entry.getKey(),
			// Integer.valueOf(entry.getValue()));
		}
		System.out
				.println("************************** After property populator ****************");

		// Way of setting the inbuilt properties e.g. Number of reducer task
		// property i.e mapred.reduce.tasks as 2.

		// job.getConfiguration().setInt("mapred.reduce.tasks", 2);

		// To set the number of splitters by setting the number of lines per
		// map.

		//job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.NLineInputFormat.class);
		//job.getConfiguration().setInt(
			//	"mapreduce.input.lineinputformat.linespermap",linesPerMapper);

		job.setJarByClass(MaxTempTR.class);
		job.setMapperClass(MaxTempMap.class);
		job.setCombinerClass(MaxTempReduce.class);
		job.setReducerClass(MaxTempReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// Can be set by method also within class and passing the value as
		// parameter.
		job.setNumReduceTasks(numberOfReduceTasks);

		Path inputPath = new Path(arg0[0]);
		Path outputPath = new Path(arg0[1]);

		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		return 0;
	}

}
