package com.wrp.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * @ClassName MapJoinMain
 * @Author LYleonard
 * @Date 2020/2/21 0:59
 * @Description TODO
 * Version 1.0
 **/
public class MapJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        URI uri = new URI("hdfs://192.168.29.110:8020/input/pdts.txt");
        Configuration conf = super.getConf();
        //添加缓存文件
        DistributedCache.addCacheFile(uri, conf);
        Job job = Job.getInstance(conf, "MapJoin");

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("E:\\develop\\Java\\bigdata-dev\\" +
                "hadoop_dev\\src\\test\\java\\com\\wrp\\hdfs\\data\\input\\join\\orders.txt"));

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(NullWritable.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///E:\\develop\\Java\\bigdata-dev\\" +
                "hadoop_dev\\src\\test\\java\\com\\wrp\\hdfs\\data\\out\\mapjoin"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MapJoinMain(), args);
        System.exit(run);
    }
}
