package com.wrp.de.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @ClassName SequenceFileRead
 * @Author LYleonard
 * @Date 2020/2/4 0:12
 * @Description TODO
 * Version 1.0
 **/
public class SequenceFileRead {
    public static void main(String[] args) {
        String uri = "hdfs://192.168.29.100:8020/test/sequenceFile";
        Configuration conf = new Configuration();
        Path path = new Path(uri);

        // Reader对象
        SequenceFile.Reader reader = null;
        try {
            SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(path);
            // 实例化Reader对象
            reader = new SequenceFile.Reader(conf, pathOption);

            // 根据反射，求出key类型
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);

            // 根据反射，求出value类型
            Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            System.out.println(position);

            while (reader.next(key, value)){
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); //从下一行记录开始

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(reader);
        }

    }
}
