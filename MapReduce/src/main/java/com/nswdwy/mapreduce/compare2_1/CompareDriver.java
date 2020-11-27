package com.nswdwy.mapreduce.compare2_1;

import com.nswdwy.mapreduce.compare1_1.CompareBean;
import com.nswdwy.mapreduce.compare1_1.CompareReducer;
import com.nswdwy.mapreduce.compare1_1.CompareMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 18:27
 */
public class CompareDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setMapperClass(CompareMapper.class);
        job.setReducerClass(CompareReducer.class);

        job.setJarByClass(CompareReducer.class);

        job.setNumReduceTasks(5);

        job.setPartitionerClass(ComparePartitioner.class);

        job.setMapOutputKeyClass(CompareBean.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(CompareBean.class);

        FileInputFormat.setInputPaths(job,new Path("d:/Z_Myself/output"));
        FileOutputFormat.setOutputPath(job,new Path("d:/Z_Myself/oooutput"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);

    }
}
