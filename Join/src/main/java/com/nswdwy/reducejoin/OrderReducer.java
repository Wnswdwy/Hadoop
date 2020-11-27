package com.nswdwy.reducejoin;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author yycstart
 * @create 2020-09-09 14:44
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable,OrderBean,NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<NullWritable> iterator = values.iterator();

        //1.第一行是品牌
        iterator.next();
        String pname = key.getPname();
        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pname);
            context.write(key,NullWritable.get());
        }
    }
}
