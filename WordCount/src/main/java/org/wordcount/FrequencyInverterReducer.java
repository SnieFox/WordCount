package org.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class FrequencyInverterReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder wordList = new StringBuilder();
        for (Text value : values) {
            wordList.append(value.toString()).append(", ");
        }
        context.write(key, new Text(wordList.toString()));
    }
}
