package com.wrp.mapreduce.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName OrderGroupPartition
 * @Author LYleonard
 * @Date 2020/2/19 15:03
 * @Description 自定义分区类
 * Version 1.0
 **/
public class OrderGroupPartition extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTasks) {
        //(key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        //注意：使用orderId作为分区条件，保证相同的orderId进入同一reduceTask中
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
