package com.wrp.de.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName CombinerUD
 * @Author LYleonard
 * @Date 2020/2/19 0:30
 * @Description 自定义Combiner类
 * Version 1.0
 **/
public class CombinerUD extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int mapCombiner = 0;
        for (IntWritable value: values){
            mapCombiner += value.get();
        }
        context.write(key, new IntWritable(mapCombiner));
    }
}
