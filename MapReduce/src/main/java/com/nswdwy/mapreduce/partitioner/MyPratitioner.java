package com.nswdwy.mapreduce.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author yycstart
 * @create 2020-09-08 14:28
 */
public class MyPratitioner extends Partitioner< Text, PratitionerBean> {

    /**
     * 136、137、138、139
     * @param text
     * @param pratitionBean
     * @param i
     * @return
     */
    @Override
    public int getPartition(Text text, PratitionerBean pratitionBean, int i) {
        String substring = text.toString().substring(0, 3);
        int partition = 0;
        switch(substring){
            case "136":
                partition = 0;
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
