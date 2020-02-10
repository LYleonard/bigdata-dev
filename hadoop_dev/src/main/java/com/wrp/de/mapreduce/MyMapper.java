package com.wrp.de.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName MyMapper
 * @Author LYleonard
 * @Date 2020/2/9 22:15
 * @Description 自定义mapper类用于WordCount例子
 *   继承Mapper，有四个泛型，
 *   keyin: k1   行偏移量 Long
 *   valuein: v1   一行文本内容   String
 *   keyout: k2   每一个单词   String
 *   valueout : v2   1  int
 *   在hadoop当中没有沿用Java的一些基本类型，使用自己封装了一套基本类型
 *   long ==> LongWritable
 *   String ==> Text
 *   int ==> IntWritable
 * Version 1.0
 **/
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    /**
     * 继承Mapper类，重新map方法，每次读取一行数据调用一次map方法
     * @param key 输入行偏移量
     * @param value 一行文本内容
     * @param context 上下文对象， 承接上面步骤发过来的数据，通过context将数据发送到下面的步骤里面去
     * @throws IOException
     * @throws InterruptedException
     *  k1   v1
     *  0   hello,world
     *
     *  k2      v2
     *  hello   1
     *  world   1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(",");
        Text text = new Text();
        IntWritable intWritable = new IntWritable(1);
        for (String word: words){
            // 将每个出现的单词都映射为1
            // key2: Text类型
            // v2: IntWritable
            text.set(word);
            // 将key2, v2写入上下文
            context.write(text, intWritable);
        }
    }
}
