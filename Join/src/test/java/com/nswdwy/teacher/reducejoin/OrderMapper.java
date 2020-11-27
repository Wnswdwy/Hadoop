package com.nswdwy.teacher.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    private OrderBean order = new OrderBean();

    private String filename;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit inputSplit = context.getInputSplit();

        FileSplit fs = (FileSplit) inputSplit;

        filename = fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        if ("order.txt".equals(filename)) {
            order.setId(fields[0]);
            order.setPid(fields[1]);
            order.setAmount(Integer.parseInt(fields[2]));
            order.setPname("");
        } else {
            order.setPid(fields[0]);
            order.setPname(fields[1]);
            order.setAmount(0);
            order.setId("");
        }

        context.write(order, NullWritable.get());


    }
}
