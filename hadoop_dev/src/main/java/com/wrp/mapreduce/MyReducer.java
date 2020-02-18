package com.wrp.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName MyReducer
 * @Author LYleonard
 * @Date 2020/2/9 22:38
 * @Description 自定义 MyReducer 类，用于WordCount例子
 * 第3步：分区   相同key的数据发送到同一个redurce里，相同key合并，value形成一个集合
 * Version 1.0
 **/
public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 继承 Redurcer 类，重写redurce方法
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable value: values){
            // 相同key的value累加
            result += value.get();
        }
        IntWritable intWritable = new IntWritable(result);
        context.write(key, intWritable);
    }
}
