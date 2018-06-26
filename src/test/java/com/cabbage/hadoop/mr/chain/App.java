package com.cabbage.hadoop.mr.chain;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



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
            job.setJobName("Chine Test Job");
            Configuration conf = job.getConfiguration();

            //删除输出目录
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(args[1]),true);


            //设置运行job的类
            job.setJarByClass(App.class);

            //设置map 输出 的K V
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置reduce 输出 的 K V
            job.setOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            //设置 输入  输出路径
            FileInputFormat.setInputPaths(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[1]));


            /************设置  chine mapper***************/
            //构造一个新的confi  不加载配置
            Configuration confA = new Configuration(false);
            ChainMapper.addMapper(job,MapperA.class,LongWritable.class,Text.class,Text.class,IntWritable.class,confA);

            Configuration confB = new Configuration(false);
            ChainMapper.addMapper(job,MapperB.class,Text.class,IntWritable.class,Text.class,IntWritable.class,confB);


            /*****************设置 reduce************************/
            Configuration confR = new Configuration(false);
            ChainReducer.setReducer(job,ReducerA.class,Text.class,IntWritable.class,Text.class,IntWritable.class,confR);

            /******************在reducer 后面继续添加mapper处理数据**********************************/
            Configuration confC = new Configuration(false);
            ChainReducer.addMapper(job,MapperC.class,Text.class,IntWritable.class,Text.class,IntWritable.class,confC);


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
