package com.cabbage.hadoop.api;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;

public class FileSystemTest {

	private String hdfs_url = "hdfs://node101:9000";

	private FileSystem fs;

	@Before
	public  void init(){
		try{
			Configuration conf = new Configuration();
			fs = FileSystem.get(new URI(hdfs_url),conf,"hadop");
		}catch(Exception e){
			e.printStackTrace();
		}
	}


	public void fsTest(){

	}



	/**
	 *写操作
	 */
	@Test
	public void writeFile() {
		Path path = new Path("/user/hadoop/test/hello.txt");
		FSDataOutputStream fos = null;
		try {
			fos = fs.create(path);
			fos.write("111111".getBytes());
			fos.close();
			System.out.println("end~~~");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 *写操作
	 */
	@Test
	public void writeFileWithReplication() {
		Path path = new Path("/user/hadoop/test/how.txt");
		try {
			FSDataOutputStream fos = fs.create(path,(short) 2);
			fos.write("how are you???".getBytes());
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 读取文件
	 */
	@Test
	public void readFile(){
		try {
			Path path = new Path("hdfs://node101:9000/user/hadoop/test/hello.txt");
			FSDataInputStream fsis =  fs.open(path);
			FileOutputStream fos = new FileOutputStream("D:/hello.txt");
			IOUtils.copyBytes(fsis,fos,1024,true);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	/**
	 * 读取文件
	 */
	@Test
	public void readFileWithSeek(){
		try {
			Path path = new Path("hdfs://node101:9000/user/hadoop/test/hello.txt");
			FSDataInputStream fsis =  fs.open(path);
			FileOutputStream fos = new FileOutputStream("D:/hello.txt");
			IOUtils.copyBytes(fsis,fos,1024,true);
		}catch (Exception e){
			e.printStackTrace();
		}
	}

}
