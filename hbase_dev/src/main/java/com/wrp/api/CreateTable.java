package com.wrp.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName CreateTable
 * @Author LYleonard
 * @Date 2020/3/8 22:52
 * @Description Hbase Java API, 操作数据库
 *          第一步：获取连接
 *          第二步：获取客户端对象
 *          第三步：操作数据库
 *          第四步：关闭
 * Version 1.0
 **/
public class CreateTable {

    /**
     * 创建一张表  myuser  两个列族  f1   f2
     */
    public void createTable(String table) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        // 连接HBase集群不需要指定HBase主节点的ip地址和端口号

        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        // 创建连接对象
        Connection connection = ConnectionFactory.createConnection(configuration);

        //获取连接对象，创建一张表
        //获取管理员对象，对数据库进行DDL的操作
        Admin admin = connection.getAdmin();

        // 指定表名
        TableName tableName = TableName.valueOf(table);
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        // 指定列簇
        HColumnDescriptor f1 = new HColumnDescriptor("f1");
        HColumnDescriptor f2 = new HColumnDescriptor("f2");
        hTableDescriptor.addFamily(f1);
        hTableDescriptor.addFamily(f2);
        admin.createTable(hTableDescriptor);
        admin.close();
        connection.close();
    }

    public static void main(String[] args) {
        try {
            String tableName = "myuser";
            CreateTable createTable = new CreateTable();
            createTable.createTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
