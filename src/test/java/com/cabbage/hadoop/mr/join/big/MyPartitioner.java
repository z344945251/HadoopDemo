package com.cabbage.hadoop.mr.join.big;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<CombineKey,Text> {

    @Override
    public int getPartition(CombineKey combineKey, Text text, int numPartitions) {
        return  combineKey.getCustomerId() % numPartitions;
    }
}
