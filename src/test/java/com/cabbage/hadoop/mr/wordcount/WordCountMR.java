package com.cabbage.hadoop.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountMR {

    public static void main(String[] args){
        try {
            //创建配置对象
            Configuration conf = new Configuration();

            //创建Job对象
            Job job = Job.getInstance(conf,"wordcount");

            //设置运行job的类
            job.setJarByClass(WordCountMR.class);

            //设置mapper类
            job.setMapperClass(WordCountMapper.class);

            //设置reducer类
            job.setReducerClass(WordCountReducer.class);

            //设置map 输出 的K V
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce 输出 的 K V
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置 输入  输出路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //提交job
            boolean b = job.waitForCompletion(true);

            if(!b){
                System.out.println("wordcount task fail!");
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
