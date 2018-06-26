package com.cabbage.hadoop.mr.sort.sampling;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler.RandomSampler;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;


public class App {

    public static void main(String[] args){
        try {
            if(args.length!=2){
                System.out.println("Usage:Job<Input path> <Output path>");
                System.exit(-1);
            }

            //创建Job对象
            Job job = Job.getInstance();
            //设置作业的名称
            job.setJobName("Test Job");
            Configuration conf = job.getConfiguration();

            //删除输出目录
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(args[1]),true);


            //采样设置

            //第一个参数表示被选中的概率
            //第二个参数是选取samples数  （采多少条数据作为样本）
            //第三个参数是指定最大的切片数
            RandomSampler sampler = new RandomSampler(0.1,10000,5);

            //设置分区文件 (是一个seq文件  key是采样的key  value是null)
            TotalOrderPartitioner.setPartitionFile(conf,new Path("path"));


            /*job.setPartitionerClass(TotalOrderPartitioner.class);
            InputSampler.RandomSampler<IntWritable,IntWritable> sampler =
                    new InputSampler.RandomSampler<IntWritable, IntWritable>(0.1,10000,10);
            InputSampler.writePartitionFile(job,sampler);
            //Add to DistributedCache
            String partitionFile = TotalOrderPartitioner.getPartitionFile(conf);
            URI partitionuri = new URI(partitionFile);
            job.addCacheFile(partitionuri);*/



            //设置运行job的类
            job.setJarByClass(App.class);

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


            //设置reduce数  不能超过采样器设置的切片书   一般情况下和sample设置是相同的
            job.setNumReduceTasks(4);

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
