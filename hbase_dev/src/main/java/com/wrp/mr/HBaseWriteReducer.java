package com.wrp.mr;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @ClassName HBaseWriteReducer
 * @Author LYleonard
 * @Date 2020/3/20 11:05
 * @Description HBase整合MR，Reducer类;TableReducer第三个泛型包含rowkey信息
 * Version 1.0
 **/
public class HBaseWriteReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
    // 把map传入的数据写入hbase表
    @Override
    protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        // rowkey
        ImmutableBytesWritable immutableBytesWritable = new ImmutableBytesWritable();
        immutableBytesWritable.set(key.toString().getBytes());

        // 遍历put对象，并输出
        for (Put put : values) {
            context.write(immutableBytesWritable, put);
        }
    }
}
