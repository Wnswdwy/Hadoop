package com.nswdwy.teacher.mapreduce.outputformats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 将KV写到两个文件中
 */
public class MyRecordWriter extends RecordWriter<LongWritable, Text> {

    private FSDataOutputStream atguigu;
    private FSDataOutputStream other;

    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        Configuration configuration = job.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        String output = configuration.get(FileOutputFormat.OUTDIR);

        atguigu = fileSystem.create(new Path(output+"/atguigu.log"));
        other = fileSystem.create(new Path(output+"/other.log"));

    }

    @Override
    public void write(LongWritable key, Text value) throws IOException, InterruptedException {
        //1. 值取出并判断是否包含atguigu
        String line = value.toString() + "\n";

        if (line.contains("atguigu")) {
            atguigu.write(line.getBytes());
        } else {
            other.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStreams(atguigu, other);
    }
}
