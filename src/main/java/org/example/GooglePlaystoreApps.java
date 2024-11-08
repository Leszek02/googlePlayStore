package org.example;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// EXECUTION COMMAND: hadoop jar mapreduce.jar org.example.GooglePlaystoreApps <INPUT_PATH> <OUTPUT_PATH>
// MY EXECUTION COMMAND: hadoop jar mapreduce.jar org.example.GooglePlaystoreApps gs://pbd-24-lm/labs/hadoop/in
//put/datasource1 gs://pbd-24-lm/labs/hadoop/input/mapreduce

public class GooglePlaystoreApps extends Configured implements Tool {

    public static void main(String[] args) {
        int res = 1;
        try{
            res = ToolRunner.run(new GooglePlaystoreApps(), args);
        } catch (Exception e) {
            System.out.println(e);
        }

        System.exit(res);
    }


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "GooglePlaystoreApps");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(GooglePlaystoreAppsMapper.class);
        job.setCombinerClass(GooglePlaystoreAppsCombiner.class);
        job.setReducerClass(GooglePlaystoreAppsReducer.class);

        job.setMapOutputKeyClass(DataKey.class);
        job.setMapOutputValueClass(DeveloperStats.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class GooglePlaystoreAppsMapper extends Mapper<LongWritable, Text, DataKey, DeveloperStats> {
        private final DoubleWritable rating = new DoubleWritable();
        private final IntWritable ratingCount = new IntWritable(-1);
        private final IntWritable applicationCreated = new IntWritable(1);
        private final LongWritable developerID = new LongWritable();
        private final Text year = new Text();
        private final DataKey dataKey = new DataKey();
        private final DeveloperStats developerStats = new DeveloperStats();

        public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
            try {
                boolean dataCorrectness = true;
                String line = lineText.toString();
                int i = 0;
                for (String word : line.split("\u0001")) {
                    if (i == 3) {
                        if (!word.equals("null")) {
                            rating.set(Double.parseDouble(word));
                        } else {
                            dataCorrectness = false;
                            break;
                        }
                    } else if (i == 4) {
                        ratingCount.set(Integer.parseInt(word));
                        if (ratingCount.get() < 1000) {
                            dataCorrectness = false;
                            break;
                        }
                    } else if (i == 13) {
                        if (word.length() > 4) {
                            year.set(word.substring(word.length() - 4));
                        } else {
                            dataCorrectness = false;
                            break;
                        }
                    } else if (i == 21) {
                        developerID.set(Long.parseLong(word));
                    }
                    i++;
                }
                if (dataCorrectness) {
                    dataKey.set(developerID, year);
                    developerStats.set(rating, ratingCount, applicationCreated);
                    context.write(dataKey, developerStats);
                }
            } catch (Exception e) {
                System.out.println(e);
            }

        }
    }

    public static class GooglePlaystoreAppsReducer extends Reducer<DataKey, DeveloperStats, Text, Text> {

        @Override
        public void reduce(DataKey key, Iterable<DeveloperStats> values, Context context) throws IOException, InterruptedException {
            double rating = 0.0d;
            int ratingCount = 0;
            int applicationCreated = 0;
            for (DeveloperStats stats : values) {
                rating += stats.GetRating().get();
                ratingCount += stats.GetRatingCount().get();
                applicationCreated += stats.GetApplicationCreated().get();
            }
            DeveloperStats finalStats = new DeveloperStats(rating, ratingCount, applicationCreated);
            Text resultKey = new Text(key.GetDeveloperID() + "," + key.GetYear());
            Text resultVal = new Text(finalStats.GetRating() + "," + finalStats.GetRatingCount() + "," + finalStats.GetApplicationCreated());
            context.write(resultKey, resultVal);
        }
    }


    public static class GooglePlaystoreAppsCombiner extends Reducer<DataKey, DeveloperStats, DataKey, DeveloperStats> {

        private final DeveloperStats developerStats = new DeveloperStats(0, 0, 0);

        @Override
        public void reduce(DataKey key, Iterable<DeveloperStats> values, Context context) throws IOException, InterruptedException {
            developerStats.set(new DoubleWritable(0), new IntWritable(0), new IntWritable(0));
            for (DeveloperStats stats : values) {
                developerStats.addStats(stats);
            }
            context.write(key, developerStats);
        }
    }
}