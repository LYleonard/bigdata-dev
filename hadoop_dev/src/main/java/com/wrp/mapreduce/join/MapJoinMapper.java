package com.wrp.mapreduce.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName MapJoinMapper
 * @Author LYleonard
 * @Date 2020/2/21 0:59
 * @Description map端join操作，适用于关联表中有小表的情形；
 * 可以将小表分发到所有的map节点，这样，map节点就可以在本地对自己所读到的大表数据进行join
 * 并输出最终结果，可以大大提高join操作的并发度，加快处理速度
 * Version 1.0
 **/
public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Map<String, String> pdtsMap;

    /**
     * 初始化方法，只在程序启动调用一次
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pdtsMap = new HashMap<String, String>();
        Configuration configuration = context.getConfiguration();

        // 获取到所有的缓存文件，但是这里只写一个缓存文件
        URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);
        // 获取到缓存文件
        URI cachefile = cacheFiles[0];
        FileSystem fileSystem = FileSystem.get(cachefile, configuration);

        //读取文件，获取到输入流。商品表的数据
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(cachefile));

//        p0001,xiaomi,1000,2
//        p0002,appale,1000,3
//        p0003,samsung,1000,4
        // 获取到BufferedReader后就可一行一行的读取数据
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null){
            String[] splits = line.split(",");
            pdtsMap.put(splits[0], line);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] splits = value.toString().split(",");
        // 获取订单表的商品id
        String pid = splits[2];
        String pdtsLine = pdtsMap.get(pid);
        context.write(new Text(value.toString()+","+pdtsLine), NullWritable.get());

    }
}
