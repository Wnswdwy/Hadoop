package com.nswdwy.teacher.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Map<String, String> pMap = new HashMap<>();

    private Text result = new Text();

    /**
     * 完成表的缓存工作,任务开始前将pd数据缓存进pMap
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //1.获取缓存文件地址
        URI[] cacheFiles = context.getCacheFiles();

        //2. 开流
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));

        //3. 转换字节流并读取 ,通过包装流转换为br,方便按行读取
        BufferedReader br = new BufferedReader(new InputStreamReader(inputStream,"UTF-8"));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line = br.readLine())) {
            String[] fields = line.split("\t");
            pMap.put(fields[0], fields[1]);
        }

        IOUtils.closeStream(br);
    }

    /**
     * 完成实际连接工作
     *
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        result.set(fields[0] + "\t" + pMap.get(fields[1]) + "\t" + fields[2]);

        context.write(result, NullWritable.get());
    }
}
