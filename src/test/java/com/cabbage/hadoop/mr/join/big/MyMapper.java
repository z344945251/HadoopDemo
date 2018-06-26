package com.cabbage.hadoop.mr.join.big;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/***
 * 订单信息
 */
public class MyMapper extends Mapper<LongWritable,Text,CombineKey,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        int flag = 0;

        //判断当前切片 是客户 还是订单
        FileSplit split = (FileSplit) context.getInputSplit();
        String path = split.getPath().toString();

        if(path.contains("customers")){
           flag = 0;
        }else if(path.contains("orders")){
            flag = 1;
        }

        String line  = value.toString();
        String[] orderInfo = line.split("\t");
        Integer custormId = Integer.valueOf(orderInfo[1]);

        CombineKey ck = new CombineKey();
        ck.setFlag(flag);
        ck.setCustomerId(custormId);

        context.write(ck,value);
    }
}
