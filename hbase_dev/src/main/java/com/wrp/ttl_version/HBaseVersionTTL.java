package com.wrp.ttl_version;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @ClassName HBaseVersionTTL
 * @Author LYleonard
 * @Date 2020/3/24 16:02
 * @Description TODO
 * Version 1.0
 **/
public class HBaseVersionTTL {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        if (!admin.tableExists(TableName.valueOf("version_hbase"))) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf("version_hbase"));
            HColumnDescriptor f1 = new HColumnDescriptor("f1");
            f1.setMinVersions(3);
            f1.setMaxVersions(5);
            // 对列簇"f1"下的所有列设置TTL
            f1.setTimeToLive(30);
            hTableDescriptor.addFamily(f1);
            admin.createTable(hTableDescriptor);
        }
        Table version_hbase = connection.getTable(TableName.valueOf("version_hbase"));
        Put put = new Put("1".getBytes());
        put.addColumn("f1".getBytes(), "name".getBytes(), System.currentTimeMillis(), "zhangsan".getBytes());
        version_hbase.put(put);
        Thread.sleep(1000);

        Put put1 = new Put("1".getBytes());
        put1.addColumn("f1".getBytes(), "name".getBytes(), System.currentTimeMillis(), "zhangsan2".getBytes());
        version_hbase.put(put1);

        Get get = new Get("1".getBytes());
        get.getMaxVersions();
        Result result = version_hbase.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
        }
        version_hbase.close();
        connection.close();
    }
}
