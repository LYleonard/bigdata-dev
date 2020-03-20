package com.wrp.api;

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
 * @ClassName PutData
 * @Author LYleonard
 * @Date 2020/3/8 23:00
 * @Description TODO
 * Version 1.0
 **/
public class PutData {

    private static void addData(){
        String tableName = "myuser";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum",
                "192.168.29.100:2181,192.168.29.101:2181,192.168.29.102:2181");

        try {
            Connection connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

            //创建put对象，并指定rowkey值
            Put putData = new Put("0001".getBytes());
            putData.addColumn("f1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
            putData.addColumn("f1".getBytes(), "age".getBytes(), Bytes.toBytes(18));
            putData.addColumn("f1".getBytes(), "id".getBytes(), Bytes.toBytes(25));
            putData.addColumn("f1".getBytes(), "address".getBytes(), Bytes.toBytes("中国人"));
            table.put(putData);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        addData();
    }
}
