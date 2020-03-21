package com.wrp.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName BulkLoadMapper
 * @Author LYleonard
 * @Date 2020/3/21 15:57
 * @Description TODO
 * Version 1.0
 **/
public class BulkLoadMapper  extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        // 封装输出的rowkey
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable(split[0].getBytes());

        // put object
        Put put = new Put(split[0].getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
        put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());

        context.write(immutableBytesWritable, put);
    }
}
