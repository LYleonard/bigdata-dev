package com.wrp.de.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName FlowSortMain
 * @Author LYleonard
 * @Date 2020/2/18 23:02
 * @Description TODO
 * Version 1.0
 **/
public class FlowSortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(FlowSortMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,
                new Path("hdfs://192.168.29.100:8020/input/data_flow.dat"));

        job.setMapperClass(FlowSortMapper.class);
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setReducerClass(FlowSortReducer.class);
        job.setOutputKeyClass(FlowSortBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,
                new Path("hdfs://192.168.29.100:8020/input/SecondarySortOut"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int exitCode = ToolRunner.run(configuration, new FlowSortMain(), args);
        System.exit(exitCode);
    }
}
