package com.wrp.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @ClassName WordCountKeyValueTextInputFormat
 * @Author LYleonard
 * @Date 2020/2/16 18:12
 * @Description KeyValueTextInputFormat允许自定义分隔符，通过分隔符来自定义key、value
 * Version 1.0
 **/
public class WordCountKeyValueTextInputFormat {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("key.value.separator.in.input.line", "@sep@");
        Job job = Job.getInstance(conf);

//        job.setJarByClass(WordCountKeyValueTextInputFormat.class);
        // 1. 读文件
        KeyValueTextInputFormat.addInputPath(job, new Path("E:\\develop\\Java\\" +
                "bigdata-dev\\hadoop_dev\\src\\test\\java\\com\\wrp\\de\\hdfs\\data\\" +
                "keyvaluetextinputformat.txt"));

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        job.setMapperClass(KeyValueMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(KeyValueReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("E:\\develop\\Java\\" +
                "bigdata-dev\\hadoop_dev\\src\\test\\java\\com\\wrp\\de\\hdfs\\data\\out"));

        boolean b = job.waitForCompletion(true);
        System.exit(0);
    }

    public static class KeyValueMapper extends Mapper<Text, Text, Text, LongWritable>{
        LongWritable outValue = new LongWritable(1);
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            context.write(key, outValue);
        }
    }

    public static class KeyValueReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value:values){
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }
}


