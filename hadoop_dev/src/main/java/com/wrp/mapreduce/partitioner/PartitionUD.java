package com.wrp.mapreduce.partitioner;

import com.wrp.mapreduce.serializable.SerializableFlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ClassName PartitionUD
 * @Author LYleonard
 * @Date 2020/2/18 22:37
 * @Description 需求：基于hadoop当中的序列化的手机流量数据，
 *                实现将不同的手机号的数据划分到不同的文件里面去.
 * Version 1.0
 **/
public class PartitionUD extends Partitioner<Text, SerializableFlowBean> {
    @Override
    public int getPartition(Text text, SerializableFlowBean serializableFlowBean, int numPartitions) {
        String phoneNum = text.toString();
        if (null != phoneNum && !phoneNum.equals("")){
            if (phoneNum.startsWith("135")){
                return 0;
            } else if (phoneNum.startsWith("136")){
                return 1;
            } else if (phoneNum.startsWith("137")){
                return 2;
            } else if (phoneNum.startsWith("138")){
                return 3;
            } else if (phoneNum.startsWith("139")){
                return 4;
            } else {
                return 5;
            }
        } else {
            return 5;
        }
    }
}
