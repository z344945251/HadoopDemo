package com.cabbage.hadoop.mr.join.big;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class MyReduce extends Reducer<CombineKey,Text,Text,NullWritable> {


    @Override
    protected void reduce(CombineKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //reduce 中的数据  是经过partition分区  和 group函数分组后的数据
        // values 中的第一条是用户数据   后面为订单    可以遍历进行join操作

    }
}
