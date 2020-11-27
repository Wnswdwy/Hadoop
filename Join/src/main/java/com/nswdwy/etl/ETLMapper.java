package com.nswdwy.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-09 20:47
 */
public class ETLMapper  extends Mapper<LongWritable,Text, Text, NullWritable> {
    private Counter pass;
    private Counter fail;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pass = context.getCounter("ETL", "Pass");
        fail = context.getCounter("ETL", "Fail");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");
        if(fields.length > 11){
            context.write(value,NullWritable.get());
            pass.increment(1);
        }else{
            fail.increment(1);
        }
    }
}
