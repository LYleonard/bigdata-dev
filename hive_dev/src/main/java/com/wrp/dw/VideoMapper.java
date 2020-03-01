package com.wrp.dw;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName VideoMapper
 * @Author LYleonard
 * @Date 2020/3/1 14:26
 * @Description 清洗放入Mapper中
 * Version 1.0
 **/
public class VideoMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    private Text key2;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        key2 = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String s = VideoUtil.washDatas(value.toString());
        if (null != s){
            key2.set(s);
            context.write(key2, NullWritable.get());
        }
    }
}
