package com.wrp.mapreduce.udinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @ClassName MyRecordReader
 * @Author LYleonard
 * @Date 2020/2/18 15:55
 * @Description TODO
 * Version 1.0
 **/
public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable bytesWritable;
    /**
     * 读取文件的标识
     */
    private boolean flag = false;

    /**
     * 初始化方法，只在初始化的时候调用一次，获取到文件的切片以及内容
     * @param inputSplit 输入的文件切片
     * @param taskAttemptContext
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.configuration = taskAttemptContext.getConfiguration();
        bytesWritable = new BytesWritable();
    }

    /**
     * 如返回true表示文件已经读取完成，不再往下读取，若为false表示文件没有读取完成，继续读取。
     * @return boolean类型
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!flag){
            long length = fileSplit.getLength();
            byte[] bytes = new byte[(int)length];
            //获取文件切片的内容
            // 文件切片路径，本地文件前缀file:///，HDFS文件系前缀hdfs://
            Path path = fileSplit.getPath();
            //获取文件系统
            FileSystem fileSystem = path.getFileSystem(configuration);
            //打开文件输入流
            FSDataInputStream open = fileSystem.open(path);

            //获取到文件输入流，将流对象封装为BytesWritable对象，inputStream ==> byte[] ==> BytesWritable
            IOUtils.readFully(open, bytes, 0, (int)length);
            bytesWritable.set(bytes, 0, (int)length);
            flag = true;
            return flag;
        }
        return false;
    }

    /**
     * 获取数据的key
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 获取数据的value
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 读取文件的进度，没什么用
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag?1.0f:0.0f;
    }

    /**
     * 关闭资源，这里可以不重写
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
