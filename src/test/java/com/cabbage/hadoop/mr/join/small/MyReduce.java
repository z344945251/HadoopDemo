package com.cabbage.hadoop.mr.join.small;

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

public class MyReduce extends Reducer<IntWritable,Text,Text,NullWritable> {


    Map<Integer,String> customerMap = new HashMap<Integer, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream dsis = fs.open(new Path("path"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(dsis));

        String  line = null;

        while ((line = reader.readLine())!=null){
            String[] info = line.split("\t");
            String id = info[0];
            customerMap.put(new Integer(id),line);
        }
        IOUtils.closeStream(reader);
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int cid = key.get();

        for (Text order:values){
            String cusInfo = customerMap.get(cid);  //获取到 用户的 info信息
            context.write(new Text(cusInfo+"\t"+order),NullWritable.get()); //将用户信息和订单信息一起输出
        }
    }
}
