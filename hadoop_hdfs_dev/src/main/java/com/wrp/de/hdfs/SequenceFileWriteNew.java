package com.wrp.de.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnsupportedFileSystemException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.util.ReflectionUtils;
import sun.reflect.misc.ReflectUtil;

import java.io.IOException;
import java.net.URI;

/**
 * @ClassName SequenceFileWriteNew
 * @Author LYleonard
 * @Date 2020/2/2 12:29
 * @Description TODO
 * Version 1.0
 **/
public class SequenceFileWriteNew {
    private static final String[] DATA = {"The Apache Hadoop software library is a framework that " +
            "allows for the distributed processing of large data sets across clusters of computers " +
            "using simple programming models.",
            "It is designed to scale up from single servers to thousands of machines, each offering " +
                    "local computation and storage.",
            "Rather than rely on hardware to deliver high-availability, the library itself is designed " +
                    "to detect and handle failures at the application layer",
            "o delivering a highly-available service on top of a cluster of computers, each of which " +
                    "may be prone to failures.",
            "Hadoop Common: The common utilities that support the other Hadoop modules."
    };

    public static void main(String[] args) {
        // 输出路径：要生成的SequenceFile文件名
        String uri = "hdfs://192.168.29.100:8020/test/sequenceFile";
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(URI.create(uri), conf);
            // 向HDFS上的SequenceFile文件写入数据
            Path path = new Path(uri);

            // SequenceFile的record是键值对，指定key与value的类型
            IntWritable key = new IntWritable();
            Text value = new Text();

//            FileContext fileContext = FileContext.getFileContext(URI.create(uri));
//            Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.SnappyCodec");
//            CompressionCodec snappyCodec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
//            SequenceFile.Metadata metadata = new SequenceFile.Metadata();
////            SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());
//            SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(path),
//                    SequenceFile.Writer.keyClass(IntWritable.class), SequenceFile.Writer.valueClass(Text.class));

            // 要写入的SequenceFile的路径
            SequenceFile.Writer.Option pathOption = SequenceFile.Writer.file(path);
            // record 的key的类型选项
            SequenceFile.Writer.Option keyOption = SequenceFile.Writer.keyClass(IntWritable.class);
            // recoed 的value的类型选项
            SequenceFile.Writer.Option valueOption = SequenceFile.Writer.valueClass(Text.class);

            // SequenceFile的压缩方式：NONE| RECORD | BLOCK， 三选一
            // 方法1：RECORD、不指定压缩算法
            SequenceFile.Writer.Option compressOption = SequenceFile.Writer.compression(SequenceFile.CompressionType.RECORD);
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, pathOption,
                    keyOption, valueOption, compressOption);

//            // 方法2：BLOCK、不指定压缩算法
//            SequenceFile.Writer.Option compressOptionBLOCK = SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK);
//            SequenceFile.Writer writer1 = SequenceFile.createWriter(conf, pathOption,
//                    keyOption, valueOption,compressOption);
//
//            // 方法3：使用BLOCK+ 压缩算法SnappyCodec；压缩耗时间
//            SnappyCodec snappyCodec = new SnappyCodec();
//            snappyCodec.setConf(conf);
//            BZip2Codec bZip2Codec = new BZip2Codec();
//            bZip2Codec.setConf(conf);
//            SequenceFile.Writer.Option compressAlgorithm =
//                    SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK,bZip2Codec);
//            // 创建写数据的Writer实例
//            SequenceFile.Writer writer2 = SequenceFile.createWriter(conf, pathOption,
//                    keyOption, valueOption, compressAlgorithm);

            for (int i=0; i < 100000; i++){
                //分别设置key、value值
                key.set(100-i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);

                // 在SequenceFile末尾追加内容
                writer.append(key, value);
            }
            IOUtils.closeStream(writer);

        } catch (UnsupportedFileSystemException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
