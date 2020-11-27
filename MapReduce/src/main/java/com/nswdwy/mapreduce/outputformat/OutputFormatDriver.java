package com.nswdwy.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-09 14:12
 */
public class OutputFormatDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OutputFormatDriver.class);

        job.setOutputFormatClass(MyOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path("d:/Z_Myself/outputformat/input"));
        FileOutputFormat.setOutputPath(job,new Path("d:/Z_Myself/outputformat/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
