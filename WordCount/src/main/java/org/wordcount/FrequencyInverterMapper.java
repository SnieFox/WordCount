package org.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class FrequencyInverterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private final static IntWritable one = new IntWritable(1);

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Deleting unnecessary characters
        line = line.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();

        context.write(one, new Text(line));
    }
}
