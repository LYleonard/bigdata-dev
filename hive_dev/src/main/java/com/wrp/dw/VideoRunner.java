package com.wrp.dw;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName VideoRunner
 * @Author LYleonard
 * @Date 2020/3/1 14:27
 * @Description ETL Wash Data
 * Version 1.0
 **/
public class VideoRunner extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(),"ETLWashData");
        job.setJarByClass(VideoRunner.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
//        TextInputFormat.addInputPath(job, new Path("file:///E:\\develop\\Java\\bigdata-dev\\hive_dev\\src\\test\\java\\data\\"));

        job.setMapperClass(VideoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(7);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
//        TextOutputFormat.setOutputPath(job, new Path("file:///E:\\develop\\Java\\bigdata-dev\\hive_dev\\src\\test\\java\\output"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new VideoRunner(), args);
        System.exit(run);
    }
}
