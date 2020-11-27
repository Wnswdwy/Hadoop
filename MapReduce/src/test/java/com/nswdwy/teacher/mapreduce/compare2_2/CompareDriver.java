package com.nswdwy.teacher.mapreduce.compare2_2;


import com.nswdwy.teacher.mapreduce.compare1_2.CompareMapper;
import com.nswdwy.teacher.mapreduce.compare1_2.CompareReducer;
import com.nswdwy.teacher.mapreduce.compare1_2.FlowBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class CompareDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);

        job.setJarByClass(CompareReducer.class);

        job.setNumReduceTasks(5);

        job.setPartitionerClass(ComparePartitioner.class);

        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path("d:/Z_Myself/output"));
        FileOutputFormat.setOutputPath(job, new Path("d:/output222"));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }
}
