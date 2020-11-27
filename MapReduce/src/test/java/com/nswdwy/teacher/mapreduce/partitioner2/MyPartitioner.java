package com.nswdwy.teacher.mapreduce.partitioner2;


import com.nswdwy.teacher.mapreduce.flows.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, FlowBean> {

/*
     * 给我们一对kv对，我们要返回他对应的分区号
     * @param text
     * @param flowBean
     * @param numPartitions
     * @return
     */

    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String start = text.toString().substring(0, 3);

        int partition = 0;

        switch (start) {
            case "136":
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
        }

        return partition;
    }
}
