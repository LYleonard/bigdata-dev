package com.wrp.mapreduce.partitioner;

import com.wrp.mapreduce.serializable.SerializableFlowBean;
import com.wrp.mapreduce.serializable.SerializableFlowMapper;
import com.wrp.mapreduce.serializable.SerializableFlowRedurcer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName SerializableFlowMain
 * @Author LYleonard
 * @Date 2020/2/11 0:18
 * @Description 序列化例子：Main类
 * Version 1.0
 **/
public class SerializableFlowMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(super.getConf(), "SerializableFlow");
        job.setJarByClass(SerializableFlowMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.29.100:8020/input/data_flow.dat"));

        job.setMapperClass(SerializableFlowMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SerializableFlowBean.class);

        job.setPartitionerClass(PartitionUD.class);
        job.setNumReduceTasks(Integer.parseInt(args[2]));// 6

        job.setReducerClass(SerializableFlowRedurcer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.29.100:8020/input/SerializableFlowOut"));
        boolean b = job.waitForCompletion(true);
        return b ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int exitStatusCode = ToolRunner.run(configuration, new SerializableFlowMain(), args);
        System.exit(exitStatusCode);
    }
}
