package com.wrp.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ClassName TableExist
 * @Author LYleonard
 * @Date 2020/3/20 17:11
 * @Description HBase Java API 判断hbase表是否存在
 * Version 1.0
 **/
public class TableExist {
    public static Configuration configuration;
    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
    }

    public boolean isTableExist(String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        return admin.tableExists(TableName.valueOf(tableName));
    }

    public static void main(String[] args) throws IOException {
        String table = "myuser2";
        TableExist tableExist = new TableExist();
        if (tableExist.isTableExist(table)) {
            System.out.println("Table has been created!");
        } else {
            System.out.println("Table hasn't been created!");
        }
    }
}
