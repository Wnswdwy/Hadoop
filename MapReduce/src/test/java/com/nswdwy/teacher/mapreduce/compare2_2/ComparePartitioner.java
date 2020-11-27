package com.nswdwy.teacher.mapreduce.compare2_2;


import com.nswdwy.teacher.mapreduce.compare1_2.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ComparePartitioner extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean flowBean, Text text, int numPartitions) {
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
