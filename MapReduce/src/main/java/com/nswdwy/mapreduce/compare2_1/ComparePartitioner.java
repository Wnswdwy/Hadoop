package com.nswdwy.mapreduce.compare2_1;

import com.nswdwy.mapreduce.compare1_1.CompareBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * @author yycstart
 * @create 2020-09-08 18:27
 */
public class ComparePartitioner extends Partitioner<CompareBean,Text> {

    @Override
    public int getPartition(CompareBean compareBean, Text text, int i) {
        String substring = text.toString().substring(0, 3);
        int partition = 0;
       switch (substring){
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
       return  partition;
    }
}
