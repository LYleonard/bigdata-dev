package com.wrp.de.udinputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @ClassName MyInputFormat
 * @Author LYleonard
 * @Date 2020/2/18 15:53
 * @Description TODO
 * Version 1.0
 **/
public class MyInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        MyRecordReader myRecordReader = new MyRecordReader();
        myRecordReader.initialize(inputSplit, taskAttemptContext);
        return myRecordReader;
    }

    /**
     * 该方法决定数据文件是否可切分，false表示去读取数据时一次性将文件全部读出
     * @param context
     * @param filename
     * @return
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
