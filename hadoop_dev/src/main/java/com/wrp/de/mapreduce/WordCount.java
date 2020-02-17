package com.wrp.de.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ClassName WordCount
 * @Author LYleonard
 * @Date 2020/2/10 22:57
 * @Description WordCount例子的MR入口
 * Version 1.0
 **/
public class WordCount extends Configured implements Tool {
    /**
     * 实现Tool接口，实现接口的run()方法, run方法用于装配程序逻辑
     * @param args
     * @return
     * @throws Exception
     * * 第一步：读取文件，解析成key,value对，k1   v1
     * * 第二步：自定义map逻辑，接受k1   v1 转换成为新的k2   v2输出
     * * 第三步：分区。相同key的数据发送到同一个reduce里面去，key合并，value形成一个集合
     * * 第四步：排序   对key2进行排序。字典顺序排序
     * * 第五步：规约 combiner过程 调优步骤 可选
     * * 第六步：分组
     * * 第七步：自定义reduce逻辑接受k2   v2 转换成为新的k3   v3输出
     * * 第八步：输出k3 v3 进行保存
     */
    @Override
    public int run(String[] args) throws Exception {
        // new 一个job对象，组装八个步骤，每个步骤是一个class类
        Configuration conf = super.getConf();
        Job job = new Job(conf, "WordCount");

        // 实际工作当中，程序运行完成之后一般都是打包到集群上面去运行，打成一个jar包
        // 如果要打包到集群上面去运行，必须添加以下设置
        job.setJarByClass(WordCount.class);

        // 第1步：读取文件，解析为key1：value1对， key1：行偏移量，value1：一行文本内容
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://192.168.29.100:8020/test/wordcount.txt"));

//        //设置输入类型为CombineTextInputFormat，设置虚拟存储切片最大值4M、每个切片处理数据量为4M
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
//        CombineTextInputFormat.addInputPath(job, new Path("hdfs://192.168.29.100:8020/test/wordcount.txt"));

        // 第2步：自定义map逻辑类
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 第3步到6步：分区，排序，规约，分组都省略

        // 第7步：自定义reduce逻辑, 设置key3 value3的类型
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 第8步：输出key3 value3. 一定要注意，输出路径是需要不存在的，如果存在就报错
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("hdfs://192.168.29.100:8020/test/wcoutput1"));

        // 提交job任务
        boolean b = job.waitForCompletion(true);
        return b ? 0:1;
    }

    /**
     * 程序主函数
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.framework.name", "local");
        conf.set("yarn.resourcemanager.hostname", "local");
        conf.set("hello", "world");

        // 提交run方法之后，得到一个程序的退出状态码
        int exitStatusCode = ToolRunner.run(conf, new WordCount(), args);
        System.exit(exitStatusCode);
    }
}
