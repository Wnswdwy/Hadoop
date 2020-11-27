package com.nswdwy.mapreduce.compare1_1;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 16:52
 */
public class CompareDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);

        job.setJarByClass(CompareReducer.class);

        job.setMapOutputKeyClass(CompareBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompareBean.class);

        FileInputFormat.setInputPaths(job,new Path("d:/Z_Myself/output"));
        FileOutputFormat.setOutputPath(job,new Path("d:/Z_Myself/ooput"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
