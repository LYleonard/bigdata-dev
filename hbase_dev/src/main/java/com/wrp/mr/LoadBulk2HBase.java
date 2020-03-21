package com.wrp.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

import java.io.IOException;

/**
 * @ClassName LoadBulk2HBase
 * @Author LYleonard
 * @Date 2020/3/21 16:39
 * @Description TODO
 * Version 1.0
 **/
public class LoadBulk2HBase {
    public static void main(String[] args) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        //获取数据库连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        //获取表的管理器对象
        Admin admin = connection.getAdmin();
        //获取table对象
        TableName tableName = TableName.valueOf("myuser2");
        Table table = connection.getTable(tableName);
        // 构建LoadIncrementalHFiles加载HFile文件
        LoadIncrementalHFiles load = new LoadIncrementalHFiles(configuration);
        load.doBulkLoad(new Path("hdfs://master:8020/hbase/out_hFile"), admin, table,
                connection.getRegionLocator(tableName));
    }

//    方式二：命令加载: 先将hbase的jar包添加到hadoop的classpath路径下
//    export HBASE_HOME=~/hbase-1.2.0-cdh5.14.2/
//    export HADOOP_HOME=~/hadoop-2.6.0-cdh5.14.2/
//    export HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase mapredcp`
//    运行命令
//    yarn jar ~/hbase-1.2.0-cdh5.14.2/lib/hbase-server-1.2.0-cdh5.14.2.jar completebulkload /hbase/out_hfile myuser2
}
