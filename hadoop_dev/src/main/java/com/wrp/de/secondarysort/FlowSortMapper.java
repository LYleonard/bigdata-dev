package com.wrp.de.secondarysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName FlowSortMapper
 * @Author LYleonard
 * @Date 2020/2/18 23:01
 * @Description TODO
 * Version 1.0
 **/
public class FlowSortMapper extends Mapper<LongWritable, Text, FlowSortBean, NullWritable> {
    private FlowSortBean flowSortBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowSortBean = new FlowSortBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        flowSortBean.setPhone(fields[0]);
        flowSortBean.setUpFlow(Integer.parseInt(fields[6]));
        flowSortBean.setDownFlow(Integer.parseInt(fields[7]));
        flowSortBean.setUpCountFlow(Integer.parseInt(fields[8]));
        flowSortBean.setDownCountFlow(Integer.parseInt(fields[9]));
        context.write(flowSortBean, NullWritable.get());
    }
}
