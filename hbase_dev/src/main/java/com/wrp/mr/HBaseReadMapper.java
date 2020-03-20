package com.wrp.mr;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @ClassName HBaseReadMapper
 * @Author LYleonard
 * @Date 2020/3/20 10:39
 * @Description  HBase整合MR，Mapper类
 *                读取HBase中myuser表的f1:name、f1:age数据，写入myuser2表的f1列族;
 *                myuser2表列族的名字要与myuser表的列族名字相同
 * Version 1.0
 **/
public class HBaseReadMapper extends TableMapper<Text, Put> {
    /**
     * @param key rowkey
     * @param value rowkey行的数据，Result类型
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取rowkey行的字节数组
        byte[] rowkeyBytes = key.get();
        String rowkeyStr = Bytes.toString(rowkeyBytes);
        Text text = new Text(rowkeyStr);

        // 输出数据， 使用Put对象写入数据
        Put put = new Put(rowkeyBytes);
        // 获取一行数据cells
        Cell[] cells = value.rawCells();
        // 将列簇f1的 name 与 age列的数据输出
        for (Cell cell:cells){
            // 取列簇，判断是否为列簇f1
            byte[] familyBytes = CellUtil.cloneFamily(cell);
            String familyStr = Bytes.toString(familyBytes);
            if ("f1".equals(familyStr)) {
                //在判断是否是name | age
                byte[] qualifierBytes = CellUtil.cloneQualifier(cell);
                String qualifierStr = Bytes.toString(qualifierBytes);
                if ("name".equals(qualifierStr)) {
                    put.add(cell);
                }
                if ("age".equals(qualifierStr)) {
                    put.add(cell);
                }
            }
        }

        // 判断是否为空，非空输出
        if (!put.isEmpty()){
            context.write(text, put);
        }
    }
}
