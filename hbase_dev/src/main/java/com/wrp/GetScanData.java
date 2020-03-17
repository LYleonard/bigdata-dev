package com.wrp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * @ClassName GetScanData
 * @Author LYleonard
 * @Date 2020/3/10 20:32
 * @Description TODO
 * Version 1.0
 **/
public class GetScanData {
    private static Connection connection;
    private static final String TABLE_NAME = "myuser";

    public static void getData(){
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",
                    "master:2181,slave1:2181,slave2:2181");
            connection= ConnectionFactory.createConnection(configuration);

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Get get = new Get(Bytes.toBytes("0003"));

            //限制只查询f1列族的所有列的值
            get.addFamily("f1".getBytes());
            //查询f2  列族 phone  这个字段
            get.addColumn("f2".getBytes(), "phone".getBytes());
            //通过get查询，返回一个result对象，所有的字段的数据都是封装在result里面
            Result result = table.get(get);
            //获取一条数据所有的cell，所有数据值都是在cell里
            List<Cell> cells = result.listCells();
            for (Cell cell: cells){
                byte[] family_name = CellUtil.cloneFamily(cell);
                byte[] column_name = CellUtil.cloneQualifier(cell);
                byte[] rowkey = CellUtil.cloneRow(cell);
                byte[] cell_value = CellUtil.cloneValue(cell);
                //判断字段的数据类型，使用对应的转换的方法，获得值
                if ("age".equals(Bytes.toString(column_name)) || "id".equals(Bytes.toString(column_name))){
                    System.out.println(Bytes.toString(family_name));
                    System.out.println(Bytes.toString(column_name));
                    System.out.println(Bytes.toString(rowkey));
                    System.out.println(Bytes.toInt(cell_value));
                } else {
                    System.out.println(Bytes.toString(family_name));
                    System.out.println(Bytes.toString(column_name));
                    System.out.println(Bytes.toString(rowkey));
                    System.out.println(Bytes.toString(cell_value));
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void scanData(){
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum",
                    "master:2181,slave1:2181,slave2:2181");
            connection= ConnectionFactory.createConnection(configuration);

            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            //没有指定startRow以及stopRow  全表扫描
            Scan scan = new Scan();
            scan.addFamily("f1".getBytes());

            scan.addColumn("f2".getBytes(), "phone".getBytes());
            scan.setStartRow("0003".getBytes());
            scan.setStopRow("0007".getBytes());

            //通过getScanner查询获取到了表里面所有的数据，是多条数据
            ResultScanner scanner = table.getScanner(scan);
            for (Result result: scanner){
                List<Cell> cells = result.listCells();
                for (Cell cell: cells){
                    byte[] family_name = CellUtil.cloneFamily(cell);
                    byte[] column_name = CellUtil.cloneQualifier(cell);
                    byte[] rowkey = CellUtil.cloneRow(cell);
                    byte[] value = CellUtil.cloneValue(cell);

                    if ("age".equals(Bytes.toString(column_name)) || "id".equals(Bytes.toString(column_name))){
                        System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)
                                +"======数据的列族为" +  Bytes.toString(family_name)
                                +"======数据的列名为" +  Bytes.toString(column_name)
                                + "==========数据的值为" +Bytes.toInt(value));
                    } else {
                        System.out.println("数据的rowkey为" +  Bytes.toString(rowkey)
                                +"======数据的列族为" +  Bytes.toString(family_name)
                                +"======数据的列名为" +  Bytes.toString(column_name)
                                + "==========数据的值为" +Bytes.toString(value));
                    }
                }
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        getData();
        scanData();
    }
}
