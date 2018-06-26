package com.cabbage.hadoop.mr.join.small;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/***
 * 订单信息
 */
public class MyMapper extends Mapper<LongWritable,Text,IntWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line  = value.toString();
        String[] orderInfo = line.split("\t");
        Integer custormId = Integer.valueOf(orderInfo[1]);
        context.write(new IntWritable(custormId),value);
    }
}
