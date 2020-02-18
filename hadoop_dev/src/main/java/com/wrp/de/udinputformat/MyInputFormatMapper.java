package com.wrp.de.udinputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName MyInputFormatMapper
 * @Author LYleonard
 * @Date 2020/2/18 15:56
 * @Description TODO
 * Version 1.0
 **/
public class MyInputFormatMapper extends Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        // 获取文件名
        String name = inputSplit.getPath().getName();
        context.write(new Text(name), value);
    }
}
