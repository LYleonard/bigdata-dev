package com.wrp.mapreduce.groupingcomparator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName OrderGroupReducer
 * @Author LYleonard
 * @Date 2020/2/19 15:24
 * @Description TODO
 * Version 1.0
 **/
public class OrderGroupReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }
}
