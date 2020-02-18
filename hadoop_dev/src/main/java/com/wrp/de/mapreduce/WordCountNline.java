package com.wrp.de.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * @ClassName WordCountNline
 * @Author LYleonard
 * @Date 2020/2/16 18:25
 * @Description NlineInputFormat定义输入的多行数据作为一个切片的数据
 * Version 1.0
 **/
public class WordCountNline {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
//        job.setJarByClass(WordCountNline.class);
        NLineInputFormat.setNumLinesPerSplit(job, 3);
        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path("E:\\develop\\Java\\bigdata-dev\\" +
                "hadoop_dev\\src\\test\\java\\com\\wrp\\de\\hdfs\\data\\nlinesplit.txt"));

        job.setMapperClass(NlineMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(NlineReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("E:\\develop\\Java\\bigdata-dev\\hadoop_dev" +
                "\\src\\test\\java\\com\\wrp\\de\\hdfs\\data\\out\\nlinewc"));

        job.waitForCompletion(true);
        System.exit(0);
    }

    public static class NlineMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word: words){
                context.write(new Text(word), new LongWritable(1));
            }
        }
    }
    public static class NlineReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value: values){
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }
}
