package com.wrp.mapreduce.Serializable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ClassName SerializableFlowMapper
 * @Author LYleonard
 * @Date 2020/2/11 0:16
 * @Description 序列化例子：自定义Mapper类
 * Version 1.0
 **/
public class SerializableFlowMapper extends Mapper<LongWritable, Text, Text, SerializableFlowBean> {
    private SerializableFlowBean flowBean;
    private Text text;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new SerializableFlowBean();
        text = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split("\t");
        String phoneNum = lines[1];
        String upFlow = lines[6];
        String downFlow = lines[7];
        String upCountFlow = lines[8];
        String downCountFlow = lines[9];

        text.set(phoneNum);
        flowBean.setUpFlow(Integer.parseInt(upFlow));
        flowBean.setDownFlow(Integer.parseInt(downFlow));
        flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));
        context.write(text, flowBean);
    }
}
