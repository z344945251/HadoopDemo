package com.cabbage.hadoop.hdfs.dataStructure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.BZip2Codec;

import java.io.IOException;

public class TestSequenceFile {

    Configuration conf = new Configuration();
    FileSystem fs;

    {
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    /**
     * 写入sequeFile
     */
    public void  write() throws  Exception{
        Path path = new Path("hdfs://fsnn:8082/user/mysequfile.seq");

        //无压缩
        /*@SuppressWarnings("deprecation")
        SequenceFile.Writer writer = SequenceFile.createWriter
                (fs,configuration,path,key.getClass(),value.getClass());*/
        //记录压缩
        @SuppressWarnings("deprecation")
        SequenceFile.Writer writer = SequenceFile.createWriter
                (fs,conf,path,IntWritable.class,
                        Text.class,SequenceFile.CompressionType.RECORD,new BZip2Codec());
        //块压缩
        /*@SuppressWarnings("deprecation")
        SequenceFile.Writer writer = SequenceFile.createWriter
                (fs,configuration,path,key.getClass(),
                value.getClass(),CompressionType.BLOCK,new BZip2Codec());*/

        IntWritable key = new IntWritable(1);
        Text text = new Text("tom");
        //写入数据
        writer.append(key,text);
        writer.close();
    }

    public static void read(String pathStr) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem fs = path.getFileSystem(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(
                pathStr), conf);

        Writable key = (Writable) ReflectionUtils.newInstance(
                reader.getKeyClass(), conf);
        Writable value = (Writable) ReflectionUtils.newInstance(
                reader.getValueClass(), conf);

        while (reader.next(key, value)) {
            System.out.printf("%s\t%s\n", key, value);
        }
        IOUtils.closeStream(reader);
    }
}
