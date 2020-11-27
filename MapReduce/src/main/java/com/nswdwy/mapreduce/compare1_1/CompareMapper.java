package com.nswdwy.mapreduce.compare1_1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 16:52
 */
public class CompareMapper extends Mapper<LongWritable, Text,CompareBean,Text> {

    private Text phone = new Text();
    private CompareBean compareBean = new CompareBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        phone.set(fields[0]);
        compareBean.set(Long.parseLong(fields[1]),Long.parseLong(fields[2]));
        context.write(compareBean,phone);
    }
}
