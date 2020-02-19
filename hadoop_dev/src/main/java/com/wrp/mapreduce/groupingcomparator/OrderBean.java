package com.wrp.mapreduce.groupingcomparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName OrderBean
 * @Author LYleonard
 * @Date 2020/2/19 12:18
 * @Description 自定义OrderBean对象
 * Version 1.0
 **/
public class OrderBean implements WritableComparable<OrderBean> {
    private String orderId;
    private Double price;

    @Override
    public int compareTo(OrderBean o) {
        // 注意：不同的订单之间，金额不需要排序
        int orderIdCompare = this.orderId.compareTo(o.orderId);
        if (orderIdCompare == 0){
            int priceCompare = this.price.compareTo(o.price);
            return -priceCompare;
        }else {
            return orderIdCompare;
        }
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeDouble(price);
    }

    /**
     * 反序列化方法
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.price = in.readDouble();
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
