package com.nswdwy.mapreduce.compare1_1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 16:52
 */
public class CompareReducer extends Reducer<CompareBean , Text,Text,CompareBean> {
    @Override
    protected void reduce(CompareBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(value,key);
        }
    }
}
