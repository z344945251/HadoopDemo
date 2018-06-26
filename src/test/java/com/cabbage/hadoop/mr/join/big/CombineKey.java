package com.cabbage.hadoop.mr.join.big;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombineKey implements WritableComparable<CombineKey> {

    private int customerId ; //客户id
    private int flag; //标记位  0：代表客户 1：代表订单

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.write(customerId);
        dataOutput.write(flag);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.customerId = dataInput.readInt();
        this.flag = dataInput.readInt();
    }

    public int compareTo(CombineKey o) {
        return  customerId - o.getCustomerId();
    }
}
