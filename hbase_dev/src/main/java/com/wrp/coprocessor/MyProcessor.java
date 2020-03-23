package com.wrp.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName MyProcessor
 * @Author LYleonard
 * @Date 2020/3/23 17:21
 * @Description HBase 协处理器
 * Version 1.0
 **/
public class MyProcessor extends BaseRegionObserver {
    /**
     * @param e
     * @param put 插入到proc1表的数据，封装在put对象里，可以解析put对象并获取数据，插入到proc2表中
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);

        // 涉及多版本问题
        List<Cell> cells = put.get("info".getBytes(), "name".getBytes());
        //获取最新版本
        Cell nameCell = cells.get(0);
//        Cell nameCell = put.get("info".getBytes(), "name".getBytes()).get(0);
        Table proc2 = connection.getTable(TableName.valueOf("proc2"));

        Put put1 = new Put(put.getRow());
        put1.add(nameCell);

        proc2.put(put1);
        proc2.close();
        connection.close();
    }
}
