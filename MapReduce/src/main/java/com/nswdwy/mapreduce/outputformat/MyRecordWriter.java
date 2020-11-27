package com.nswdwy.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-09 14:17
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration configuration = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        String output = configuration.get(FileOutputFormat.OUTDIR);

        atguigu = fileSystem.create(new Path(output + "/atguigu.log"));
        other = fileSystem.create(new Path(output + "/other.log"));

    }

    @Override
    public void write(LongWritable longWritable, Text text) throws IOException, InterruptedException {
        //1. 值取出并判断是否包含atguigu
        String str = text.toString() + "\t";
        if(str.contains("atguigu")){
            atguigu.write(str.getBytes());
        }else{
            other.write(str.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }
}
