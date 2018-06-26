package com.cabbage.hadoop.mr.sort.secondary;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/***
 * 年度组对比器  将年度相同的数据分在同一个组
 */
public class YearGroupComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombineKey key1 = (CombineKey) a;
        CombineKey key2 = (CombineKey) b;

        return key1.getYear() - key2.getYear();
    }
}
