package com.nswdwy.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author yycstart
 * @create 2020-09-09 14:56
 */
public class OrderBean implements WritableComparable<OrderBean> {
    private String id;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public String toString() {
        return id + "\t" + pid + "\t" + amount + "\t" + pname;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
    }

    @Override
    public int compareTo(OrderBean o) {
        int compare = this.id.compareTo(o.id);
        if(compare ==0){
            return o.getPname().compareTo(this.pname);
        }else{
            return compare;
        }
    }
}
