package com.wrp.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName BulkLoadMain
 * @Author LYleonard
 * @Date 2020/3/21 16:15
 * @Description TODO
 * Version 1.0
 **/
public class BulkLoadMain  extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        int run = ToolRunner.run(configuration, new BulkLoadMain(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf, "BulkLoad");
        job.setJarByClass(BulkLoadMain.class);

        FileInputFormat.addInputPath(job, new Path("hdfs://master:8020/hbase/input"));
        job.setMapperClass(BulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("myuser2"));

        // 使用MR增量写入数据
        HFileOutputFormat2.configureIncrementalLoad(job, table,
                connection.getRegionLocator(TableName.valueOf("myuser2")));
        // 数据写回到HDFS， 写成HFile，因此指定输出格式为HFileOutputFormat2
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HFileOutputFormat2.setOutputPath(job, new Path("hdfs://master:8020/hbase/out_hFile"));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
}
