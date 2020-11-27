package com.nswdwy.teacher.mapreduce.wordcount;






import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-07 11:14
 */
public class WcMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text word = new Text();
    private IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1. 按空格切分
        String[] words = value.toString().split(" ");

        for (String word : words) {
            //2. 包装
            this.word.set(word);
            //3. 输出
            context.write(this.word, this.one);

        }
    }
}
