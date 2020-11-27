package com.nswdwy.mapreduce.partitioner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 14:05
 */
public class PartitionerMapper extends Mapper<LongWritable, Text,Text, PratitionerBean> {

    private Text phone = new Text();
    private PratitionerBean pratition = new PratitionerBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        phone.set(fields[1]);
        pratition.set(Long.parseLong(fields[fields.length-3]),Long.parseLong(fields[fields.length-2]));

        context.write(phone,pratition);
    }
}
