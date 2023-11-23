package org.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "WordCount");

        job1.setJarByClass(WordCountRunner.class);
        job1.setMapperClass(WordCountMapper.class);
        job1.setReducerClass(WordCountReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        boolean job1Success = job1.waitForCompletion(true);

        if (job1Success) {
            // Второй цикл MapReduce
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "FrequencyInverter");

            job2.setJarByClass(WordCountRunner.class);
            job2.setMapperClass(FrequencyInverterMapper.class);
            job2.setReducerClass(FrequencyInverterReducer.class);

            job2.setOutputKeyClass(IntWritable.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));  // Используем выходные данные первой задачи как входные для второй
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
