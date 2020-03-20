package com.wrp.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * @ClassName HDFS2HBase
 * @Author LYleonard
 * @Date 2020/3/20 23:41
 * @Description 将HDFS上文件/hbase/input/user.txt数据，导入到HBase的myuser2表
 * Version 1.0
 **/
public class HDFS2HBase {
    public static class HdfsMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        //数据原样输出
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class HBaseReducer extends TableReducer<Text, NullWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            /**
             * key -> 一行数据
             *  样例数据：
             *  0007	zhangsan	18
             */
            String[] split = key.toString().split("\t");
            Put put = new Put(Bytes.toBytes(split[0]));

            put.addColumn("f1".getBytes(), "name".getBytes(), split[1].getBytes());
            put.addColumn("f1".getBytes(), "age".getBytes(), split[2].getBytes());

            context.write(new ImmutableBytesWritable(Bytes.toBytes(split[0])), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        Job job = Job.getInstance(configuration, "HDFS2HBase");
        job.setJarByClass(HDFS2HBase.class);
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://master:8020/hbase/input"));

        job.setMapperClass(HdfsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 指定输出到Hbase的表名
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseReducer.class,job);
        // 设置reducer数量
        job.setNumReduceTasks(1);
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}
