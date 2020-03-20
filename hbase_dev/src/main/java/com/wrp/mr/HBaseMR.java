package com.wrp.mr;

import com.wrp.api.CreateTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @ClassName HBaseMR
 * @Author LYleonard
 * @Date 2020/3/20 11:20
 * @Description HBase整合MR，入口类
 *               读取HBase中myuser表的f1:name、f1:age数据，写入myuser2表的f1列族;
 *  *            myuser2表列族的名字要与myuser表的列族名字相同
 * Version 1.0
 **/
public class HBaseMR extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
//        String table = "myuser2";
//        CreateTable createTable = new CreateTable();
//        try {
//            System.out.printf("=============创建表：%s =================", table);
//            createTable.createTable(table);
//            System.out.printf("=============创建表：%s 完成==============", table);
//        } catch (IOException e) {
//            System.out.printf("=============创建表：%s 错误！============", table);
//            e.printStackTrace();
//        }

        System.out.println("=============执行HBase MR程序============");
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
        int run = ToolRunner.run(configuration, new HBaseMR(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = super.getConf();
        Job job = Job.getInstance(configuration, "HBaseMR");
        job.setJarByClass(HBaseMR.class);

        // mapper
        TableMapReduceUtil.initTableMapperJob(TableName.valueOf("myuser"),
                new Scan(), HBaseReadMapper.class, Text.class, Put.class, job);

        // reducer
        TableMapReduceUtil.initTableReducerJob("myuser2",
                HBaseWriteReducer.class, job);

        boolean b = job.waitForCompletion(true);

        return b ? 0:1;
    }
}
