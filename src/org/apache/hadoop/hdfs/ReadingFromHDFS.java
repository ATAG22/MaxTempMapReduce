package org.apache.hadoop.hdfs;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;



public class ReadingFromHDFS {

	public static void main(String[] args) throws IOException {
		String uri = args[0]; 
		
		Configuration configuration = new Configuration();
		System.out.println("Trying to get the file system object");
		URI uriObj = URI.create(uri);
		System.out.println("Got URI object "+uri);
		FileSystem fs = FileSystem.get(uriObj, configuration);
		FSDataInputStream fsDataInputStream = null;
		
		Path hdfsPath = new Path(uri);
		
		fsDataInputStream = fs.open(hdfsPath);
		// This specifies the reading starts from the 0th Byte.
		fsDataInputStream.seek(0);
		IOUtils.copyBytes(fsDataInputStream, System.out, 4096, false);
		System.out.println("*******************************************");

		BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(hdfsPath)));
		
		try {
			  String line;
			  line=br.readLine();
			  while (line != null){
			    System.out.println("################ Line is###### "+line);
			    // be sure to read the next line otherwise you'll get an infinite loop
			    line = br.readLine();
			  }
			} finally {
			  // you should close out the BufferedReader
			  br.close();
			}
	}

}
