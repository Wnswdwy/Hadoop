package com.nswdwy.teacher.mapreduce.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:/Z_Myself/wordinput","d:/Z_Myself/wordoutput12"};
        //1. 生成一个Job对象
        Configuration configuration = new Configuration();

//        configuration.setBoolean("mapreduce.output.fileoutputformat.compress",true);
//        configuration.setClass("mapreduce.output.fileoutputformat.compress.codec", BZip2Codec.class, CompressionCodec.class);
//        configuration.set("mapreduce.job.queuename","hive");
//        //设置HDFS NameNode的地址
//        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
//        // 指定MapReduce运行在Yarn上
//        configuration.set("mapreduce.framework.name","yarn");
//        // 指定mapreduce可以在远程集群运行
//        configuration.set("mapreduce.app-submission.cross-platform","true");
//        //指定Yarn resourcemanager的位置
//        configuration.set("yarn.resourcemanager.hostname","hadoop103");


        Job job = Job.getInstance(configuration);

        //2. 设置这个Jar包位置
        job.setJarByClass(WcDriver.class);

//        job.setJar("D:\\_StudyAll\\Java_All\\IDEA_All\\_MyFiles\\Hadoop\\MapReduce\\target\\MapReduce-1.0-SNAPSHOT.jar");
        //3. 设置我们写的 Mapper和Reducer
        job.setMapperClass(WcMapper.class);
        job.setReducerClass(WcReducer.class);

        //4. 设置输入输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //5. 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //6. 提交任务
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);

    }
}