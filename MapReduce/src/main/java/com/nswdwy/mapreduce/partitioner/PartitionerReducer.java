package com.nswdwy.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-08 14:07
 */
public class PartitionerReducer extends Reducer<Text, PratitionerBean,Text, PratitionerBean> {

    PratitionerBean result =  new PratitionerBean();
    @Override
    protected void reduce(Text key, Iterable<PratitionerBean> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        for (PratitionerBean value : values) {
            up += value.getUpFlow();
            down += value.getDownFlow();
        }
        result.set(up,down);
        context.write(key,result);
    }
}
