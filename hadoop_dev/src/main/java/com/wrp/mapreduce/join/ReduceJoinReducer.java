package com.wrp.mapreduce.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName ReduceJoinReducer
 * @Author LYleonard
 * @Date 2020/2/21 0:15
 * @Description TODO
 * Version 1.0
 **/
public class ReduceJoinReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String order = "";
        String product = "";

        for (Text value: values){
            if (value.toString().startsWith("p")){
                product = value.toString();
            } else {
                order = value.toString();
            }
        }
        context.write(new Text(order + "," + product), NullWritable.get());
    }
}
