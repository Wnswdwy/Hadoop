package com.nswdwy.mapreduce.partitioner;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 14:08
 */
public class PartitionerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(PartitionerDriver.class);

        job.setMapperClass(PartitionerMapper.class);
        job.setReducerClass(PartitionerReducer.class);

        job.setNumReduceTasks(5);

        job.setPartitionerClass(MyPratitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PratitionerBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PratitionerBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/Z_Myself/input"));
        FileOutputFormat.setOutputPath(job, new Path("d:/Z_Myself/output"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
