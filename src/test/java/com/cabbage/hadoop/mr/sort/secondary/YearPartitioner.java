package com.cabbage.hadoop.mr.sort.secondary;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/***
 * 按照组合key中的year进行分区
 */
public class YearPartitioner extends Partitioner<CombineKey,NullWritable> {


    public int getPartition(CombineKey key, NullWritable nullWritable, int numPartitions) {
        return  key.getYear()%numPartitions;
    }
}
