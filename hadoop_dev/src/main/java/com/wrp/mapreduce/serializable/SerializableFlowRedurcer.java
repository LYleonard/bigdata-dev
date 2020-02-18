package com.wrp.mapreduce.serializable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ClassName SerializableFlowRedurcer
 * @Author LYleonard
 * @Date 2020/2/11 0:17
 * @Description 序列化例子：自定义Redurcer类
 * Version 1.0
 **/
public class SerializableFlowRedurcer extends Reducer<Text,SerializableFlowBean, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<SerializableFlowBean> values, Context context) throws IOException, InterruptedException {
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for (SerializableFlowBean value: values){
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        context.write(key, new Text(upFlow + "\t" + downFlow + "\t"
                + upCountFlow + "\t" + downCountFlow));
    }
}
