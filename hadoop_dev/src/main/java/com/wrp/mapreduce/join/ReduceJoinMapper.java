package com.wrp.mapreduce.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName ReduceJoinMapper
 * @Author LYleonard
 * @Date 2020/2/21 0:11
 * @Description TODO
 * Version 1.0
 **/
public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, Text> {
    /**
     * 读取两个文件，如何确定当前处理的这一行数据是来自哪一个文件里面的
     * @param key
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

//        //获取文件的切片，通过文件名判断
//        FileSplit fileSplit = (FileSplit) context.getInputSplit();
//        String filename = fileSplit.getPath().getName();
//        if (filename.equals("order.txt")){
//            // 订单数据
//        } else {
//            // 商品数据
//        }

        String[] fields = value.toString().split(",");
        if (value.toString().startsWith("p")){
            //以商品id作为key2,相同商品的数据都会到一起去
            context.write(new Text(fields[0]), value);
        } else {
            context.write(new Text(fields[2]), value);
        }
    }
}
