package com.wrp.mapreduce.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ClassName OrderGroup
 * @Author LYleonard
 * @Date 2020/2/19 15:11
 * @Description TODO
 * Version 1.0
 **/
public class OrderGroup extends WritableComparator {
    /**
     * 重写构造方法， 通过反射构造OrderBean对象，
     * 接收的key为OrderBean对象类型,需要告诉分组，以OrderBean接受传入的参数
     */
    public OrderGroup(){
        super(OrderBean.class, true);
    }

    /**
     * compare方法接受到两个参数，这两个参数其实就是前面传过来的OrderBean
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean first = (OrderBean) a;
        OrderBean second = (OrderBean) b;
        return first.getOrderId().compareTo(second.getOrderId());
    }
}
