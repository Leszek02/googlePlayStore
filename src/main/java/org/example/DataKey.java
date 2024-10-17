package org.example;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataKey implements WritableComparable<DataKey> {

    LongWritable developerID;
    Text year;

    public DataKey(){
        set(new LongWritable(0), new Text());
    }

    public DataKey(long developerID, String year) {
        this.developerID = new LongWritable(developerID);
        this.year = new Text(year);
    }

    public void set(LongWritable developerID, Text year) {
        this.developerID = developerID;
        this.year = year;
    }

    public LongWritable GetDeveloperID() {
        return this.developerID;
    }

    public Text GetYear() {
        return this.year;
    }

    @Override
    public int compareTo(DataKey dataKey) {
        int comparison = developerID.compareTo(dataKey.developerID);
        if (comparison != 0) {
            return comparison;
        }
        return year.compareTo(dataKey.year);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.developerID.write(dataOutput);
        this.year.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.developerID.readFields(dataInput);
        this.year.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataKey dataKey = (DataKey) o;
        return developerID.equals(dataKey.developerID) && year.equals(dataKey.year);
    }

    @Override
    public int hashCode() {
        int result = developerID.hashCode();
        result = 31 * result + year.hashCode();
        return result;
    }
}
