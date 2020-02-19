package com.wrp.mapreduce.groupingcomparator;

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
 * @ClassName OrderGroupMain
 * @Author LYleonard
 * @Date 2020/2/19 15:29
 * @Description TODO
 * Version 1.0
 **/
public class OrderGroupMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, "GroupingComparator");

        // 1：读取文件，解析成为key\value对
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///E:\\develop\\Java\\" +
                "bigdata-dev\\hadoop_dev\\src\\test\\java\\com\\wrp\\hdfs\\data\\orders.txt"));

        // 2.自定义map逻辑
        job.setMapperClass(OrderGroupMapper.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 3.分区
        job.setPartitionerClass(OrderGroupPartition.class);
        // 4.排序  已经做了
        // 5.规约  combiner  省掉
        // 6.分组   自定义分组逻辑
        job.setGroupingComparatorClass(OrderGroup.class);

        // 7.设置reduce逻辑
        job.setReducerClass(OrderGroupReducer.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        // 8.设置输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///E:\\develop\\Java\\" +
                "bigdata-dev\\hadoop_dev\\src\\test\\java\\com\\wrp\\hdfs\\data\\out\\" +
                "groupingcomparator"));

        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        int exitCode = ToolRunner.run(configuration, new OrderGroupMain(), args);
        System.exit(exitCode);
    }
}
