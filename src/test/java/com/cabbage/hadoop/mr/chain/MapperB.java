package com.cabbage.hadoop.mr.chain;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/***
 * 第二个mapper
 */
public class MapperB extends Mapper<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(key,value);
    }
}
