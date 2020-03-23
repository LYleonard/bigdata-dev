package com.wrp.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName TestObserver
 * @Author LYleonard
 * @Date 2020/3/24 22:35
 * @Description  向proc1表添加数据
 * Version 1.0
 **/
public class TestObserver {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);

        Table proc1 = connection.getTable(TableName.valueOf("proc1"));
        Put put = new Put(Bytes.toBytes("hello_word"));
        put.addColumn(Bytes.toBytes("info"), "name".getBytes(), "helloword".getBytes());
        put.addColumn(Bytes.toBytes("info"), "gender".getBytes(), "femal".getBytes());
        put.addColumn(Bytes.toBytes("info"), "nationality".getBytes(), "China".getBytes());

        proc1.put(put);
        byte[] row = put.getRow();
        System.out.println(Bytes.toString(row));
        proc1.close();
        connection.close();
    }
}
