package com.nswdwy.teacher.mapreduce.compare1_2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CompareMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    private Text phone = new Text();
    private FlowBean flow = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        phone.set(fields[0]);

        flow.set(
                Long.parseLong(fields[1]),
                Long.parseLong(fields[2])
        );

        context.write(flow, phone);
    }
}
