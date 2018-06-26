package com.cabbage.hadoop.mr.sort.secondary;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/***
 * 二次排序
 *
 */
public class App {

    public static void main(String[] args){
        try {


            //创建Job对象
            Job job = Job.getInstance();

            //创建配置对象
            Configuration conf = job.getConfiguration();

            job.setJobName("Test Job");

            //设置运行job的类
            job.setJarByClass(App.class);

            //设置mapper类
            job.setMapperClass(WordCountMapper.class);

            //设置reducer类
            job.setReducerClass(WordCountReducer.class);


            //设置reduce 输出 的 K V
            job.setOutputKeyClass(CombineKey.class);
            job.setOutputKeyClass(NullWritable.class);

            //设置分区函数
            job.setPartitionerClass(YearPartitioner.class);
            //设置Key的排序方式 在到达reduce之前排序
            //job.setSortComparatorClass(xxx);

            //设置组对比器
            job.setGroupingComparatorClass(YearGroupComparator.class);

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
