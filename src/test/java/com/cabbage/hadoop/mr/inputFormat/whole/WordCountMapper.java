package com.cabbage.hadoop.mr.inputFormat.whole;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class WordCountMapper extends Mapper<NullWritable,BytesWritable,Text,IntWritable> {

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        byte[] data = value.copyBytes();
        String str = new String(data,"utf8");
        str.replace("\r\n"," ");
        str.split(" ");
    }
}
