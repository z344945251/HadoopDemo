package com.cabbage.hadoop.mr.sort.secondary;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CombineKey implements WritableComparable<CombineKey> {

    private int year;
    private int temp;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getTemp() {
        return temp;
    }

    public void setTemp(int temp) {
        this.temp = temp;
    }

    public void write(DataOutput out) throws IOException {
        out.write(year);
        out.write(temp);
    }

    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.temp = in.readInt();
    }

     public int compareTo(CombineKey o) {
         int oyear = o.getYear();
         int otemp = o.getTemp();
         if(this.year!=oyear){
             return  this.year - oyear;
         }else {
             return  otemp - temp;
         }
     }
}
