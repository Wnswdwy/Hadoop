package com.nswdwy.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;

public class MJDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1 获取job信息
        Job job = Job.getInstance(new Configuration());
        // 2 设置加载jar包路径
        job.setJarByClass(MJDriver.class);
        // 3 关联mapper
        job.setMapperClass(MJMapper.class);
        // 4 设置Map输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出KV类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        job.addCacheFile(URI.create("file:///d:/Z_Myself/join/myself/mapjoin/input/pd.txt"));
        job.setNumReduceTasks(0);

        FileInputFormat.setInputPaths(job, new Path("d:/Z_Myself/join/myself/mapjoin/input/order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:/Z_Myself/join/myself/mapjoin/output2"));
        // 7 提交
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
