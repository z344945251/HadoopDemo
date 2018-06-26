package com.cabbage.hadoop.mr.join.big;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyGroupComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        CombineKey k1 = (CombineKey) a;
        CombineKey k2 = (CombineKey) b;
        return  k1.getCustomerId() - k2.getCustomerId();
    }
}
