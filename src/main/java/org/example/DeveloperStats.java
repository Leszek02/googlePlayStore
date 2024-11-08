package org.example;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DeveloperStats implements WritableComparable<DeveloperStats> {

    DoubleWritable rating;
    IntWritable ratingCount;
    IntWritable applicationCreated;

    public DeveloperStats(){
        set(new DoubleWritable(0), new IntWritable(0), new IntWritable(0));
    }

    public DeveloperStats(double rating, int ratingCount, int applicationCreated) {
        this.rating = new DoubleWritable(rating);
        this.ratingCount = new IntWritable(ratingCount);
        this.applicationCreated = new IntWritable(applicationCreated);
    }


    public void set(DoubleWritable review, IntWritable reviewCount, IntWritable applicationCreated) {
        this.rating = review;
        this.ratingCount = reviewCount;
        this.applicationCreated = applicationCreated;
    }

    public DoubleWritable GetRating() {
        return this.rating;
    }

    public IntWritable GetRatingCount() {
        return this.ratingCount;
    }

    public IntWritable GetApplicationCreated() {
        return this.applicationCreated;
    }

    public void addStats(DeveloperStats stats) {
        this.rating = new DoubleWritable(this.rating.get() + stats.GetRating().get());
        this.ratingCount = new IntWritable(this.ratingCount.get() + stats.GetRatingCount().get());
        this.applicationCreated = new IntWritable(this.applicationCreated.get() + stats.GetApplicationCreated().get());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.rating.write(dataOutput);
        this.ratingCount.write(dataOutput);
        this.applicationCreated.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.rating.readFields(dataInput);
        this.ratingCount.readFields(dataInput);
        this.applicationCreated.readFields(dataInput);
    }

    @Override
    public int compareTo(DeveloperStats developerStats) {
        int comparison = rating.compareTo(developerStats.rating);
        if (comparison != 0) {
            return comparison;
        }
        comparison = ratingCount.compareTo(developerStats.ratingCount);
        if (comparison != 0) {
            return comparison;
        }
        return applicationCreated.compareTo(developerStats.applicationCreated);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DeveloperStats developerStats = (DeveloperStats) o;
        return rating.equals(developerStats.rating) && ratingCount.equals(developerStats.ratingCount) && applicationCreated.equals(developerStats.applicationCreated);
    }

    @Override
    public int hashCode() {
        int result = rating.hashCode();
        result = 31 * result + ratingCount.hashCode();
        result = 31 * result + applicationCreated.hashCode();
        return result;
    }

}
