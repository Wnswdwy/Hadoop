package com.nswdwy.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * @author yycstart
 * @create 2020-09-09 14:57
 */
public class OrderComparator extends WritableComparator {
    protected OrderComparator() {
        super(OrderBean.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean oa = (OrderBean) a;
        OrderBean ob = (OrderBean) b;
        return oa.getPid().compareTo(ob.getPid());
    }
}
