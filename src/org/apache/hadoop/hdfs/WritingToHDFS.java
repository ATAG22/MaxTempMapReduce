package org.apache.hadoop.hdfs;


import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

public class WritingToHDFS {

	public static void main(String[] args) throws IOException {
		String localSourcePath = args[0]; 
		String  hdfsURI = args[1];
		
		Configuration configuration = new Configuration();
		System.out.println("Configuration object is "+configuration.toString());
		System.out.println("Trying to get the file system object");
		
		URI uriObj = URI.create(hdfsURI);
		System.out.println("Got URI object "+hdfsURI);
		
		FileSystem fs = FileSystem.get(uriObj, configuration);
		
		InputStream fis = new FileInputStream(localSourcePath);
		InputStream localFileInputStream = new BufferedInputStream(fis);
		
		Path pathOfHDFS = new Path(hdfsURI);
		FSDataOutputStream hdfsDataOutputStream = fs.create(pathOfHDFS, new Progressable() {
			
			public void progress() {
			System.out.println("Copying in progress..");
				
			}
		});
		
	   IOUtils.copyBytes(localFileInputStream, hdfsDataOutputStream, configuration, true);

	}

}
