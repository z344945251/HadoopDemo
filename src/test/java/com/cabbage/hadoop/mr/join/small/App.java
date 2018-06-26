package com.cabbage.hadoop.mr.join.small;

import com.cabbage.hadoop.mr.wordcount.WordCountMapper;
import com.cabbage.hadoop.mr.wordcount.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class App {

    public static void main(String[] args){
        try {
            //创建配置对象
            Configuration conf = new Configuration();

            //创建Job对象
            Job job = Job.getInstance(conf,"wordcount");

            //设置运行job的类
            job.setJarByClass(App.class);

            //设置mapper类
            job.setMapperClass(MyMapper.class);

            //设置reducer类
            job.setReducerClass(MyReduce.class);

            //设置reduce 输出 的 K V
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);

            //设置 输入  输出路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));

            //设置reduce个数
            job.setNumReduceTasks(2);

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
