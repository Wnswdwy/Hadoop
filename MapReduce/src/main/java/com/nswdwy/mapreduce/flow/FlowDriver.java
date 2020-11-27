package com.nswdwy.mapreduce.flow;






import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-07 18:14
 */
public class FlowDriver  {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //-DHADOOP_USER_NAME=atguigu
        //1. 生成一个Job对象
          Configuration configuration = new Configuration();
        /*//设置HDFS NameNode的地址
        configuration.set("fs.defaultFS", "hdfs://hadoop102:9820");
        // 指定MapReduce运行在Yarn上
        configuration.set("mapreduce.framework.name","yarn");
        // 指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform","true");
        //指定Yarn resourcemanager的位置
        configuration.set("yarn.resourcemanager.hostname","hadoop103");*/

        Job job = Job.getInstance(configuration);

        //2. 设置这个Jar包位置
//        job.setJarByClass(FlowDriver.class);
        job.setJar("D:\\_StudyAll\\Java_All\\IDEA_All\\_MyFiles\\Maven\\MapReduce\\target\\com.nswdwy.maven-1.0-SNAPSHOT.jar");

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//        FileInputFormat.setInputPaths(job,new Path("d:/Z_Myself/input"));
//        FileOutputFormat.setOutputPath(job,new Path("d:/Z_Myself/output"));

        boolean b = job.waitForCompletion(true);
        System.exit(b?0:1);
    }
}
