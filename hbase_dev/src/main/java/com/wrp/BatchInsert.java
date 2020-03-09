package com.wrp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName BatchInsert
 * @Author LYleonard
 * @Date 2020/3/8 23:25
 * @Description 批量插入数据
 * Version 1.0
 **/
public class BatchInsert {
    private static void batchInsert(){
        String tableName = "myuser";
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
            Table table = connection.getTable(TableName.valueOf(tableName));

            //创建put对象，并指定rowkey
            Put put = new Put("0002".getBytes());

            // f1 列簇
            put.addColumn("f1".getBytes(),"id".getBytes(), Bytes.toBytes(1));
            put.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("lisi"));
            put.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(30));

            //f2
            put.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
            put.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("guiyang"));
            put.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("16888888888"));
            put.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("helloworld"));

            Put put2 = new Put("0003".getBytes());
            put2.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(2));
            put2.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("wangwu"));
            put2.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(32));
            put2.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
            put2.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("guizhou"));
            put2.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("17888888888"));
            put2.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("talk is cheap , show me the code"));

            Put put3 = new Put("0004".getBytes());
            put3.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(3));
            put3.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("liufang"));
            put3.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(35));
            put3.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
            put3.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("zunyi"));
            put3.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("12888888888"));
            put3.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("what are you doing！"));

            Put put4 = new Put("0005".getBytes());
            put4.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(4));
            put4.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("zhuge"));
            put4.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
            put4.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
            put4.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("sichuan"));
            put4.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("14888888888"));

            Put put5 = new Put("0006".getBytes());
            put5.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
            put5.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("tiantian"));
            put5.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(27));
            put5.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
            put5.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("beijing"));
            put5.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15888888888"));
            put5.addColumn("f2".getBytes(),"say".getBytes(),Bytes.toBytes("hahahah"));


            Put put6 = new Put("0007".getBytes());
            put6.addColumn("f1".getBytes(),"id".getBytes(),Bytes.toBytes(5));
            put6.addColumn("f1".getBytes(),"name".getBytes(),Bytes.toBytes("caoer"));
            put6.addColumn("f1".getBytes(),"age".getBytes(),Bytes.toBytes(28));
            put6.addColumn("f2".getBytes(),"sex".getBytes(),Bytes.toBytes("1"));
            put6.addColumn("f2".getBytes(),"address".getBytes(),Bytes.toBytes("zhejing"));
            put6.addColumn("f2".getBytes(),"phone".getBytes(),Bytes.toBytes("15788888888"));

            List<Put> listPut = new ArrayList<Put>();
            listPut.add(put);
            listPut.add(put2);
            listPut.add(put3);
            listPut.add(put4);
            listPut.add(put5);
            listPut.add(put6);

            table.put(listPut);
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        batchInsert();
    }
}
