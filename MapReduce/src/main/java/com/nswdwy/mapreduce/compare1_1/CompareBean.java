package com.nswdwy.mapreduce.compare1_1;




import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author yycstart
 * @create 2020-09-08 16:52
 */
public class CompareBean implements WritableComparable<CompareBean> {
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }
    public void set(long upFlow,long downFlow){
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 序列化
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     * 反序列化
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    /**
     * 总流量倒序
     * @param o
     * @return
     */
    @Override
    public int compareTo(CompareBean o) {
        return Long.compare(o.sumFlow,this.sumFlow);
    }
}
