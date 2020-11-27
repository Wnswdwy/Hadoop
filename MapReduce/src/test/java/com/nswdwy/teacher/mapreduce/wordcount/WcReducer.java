package com.nswdwy.teacher.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author yycstart
 * @create 2020-09-07 11:14
 */
public class WcReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //1. 遍历values，累加
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        //2. 封装，输出
        result.set(sum);
        context.write(key, result);
    }
}
