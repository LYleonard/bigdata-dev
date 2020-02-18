package com.wrp.mapreduce.udinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName MyInputFormatMain
 * @Author LYleonard
 * @Date 2020/2/18 15:57
 * @Description TODO
 * Version 1.0
 **/
public class MyInputFormatMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, "MergeSmallFilebyUDFileInputFormat");
        job.setInputFormatClass(MyInputFormat.class);
        MyInputFormat.addInputPath(job, new Path("E:\\develop\\Java\\bigdata-dev\\" +
                "hadoop_dev\\src\\test\\java\\com\\wrp\\de\\hdfs\\data\\inputsmallfiles"));

        job.setMapperClass(MyInputFormatMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // SequenceFile格式输出
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job, new Path("E:\\develop\\Java\\" +
                "bigdata-dev\\hadoop_dev\\src\\test\\java\\com\\wrp\\de\\hdfs\\data\\out\\" +
                "myinputformatout"));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int exitCode = ToolRunner.run(configuration, new MyInputFormatMain(), args);
        System.exit(exitCode);
    }
}
