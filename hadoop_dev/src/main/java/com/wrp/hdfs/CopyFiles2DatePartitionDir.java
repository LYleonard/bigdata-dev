package com.wrp.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @ClassName CopyFiles2DatePartitionDir
 * @Author LYleonard
 * @Date 2020/4/22 22:38
 * @Description 需求，原Hadoop集群中Hive的ORC+Snappy外部表未做分区，
 *               且原集群每个数据表的数据量已达数TB级别，
 *               现需要将orc文件根据修改时间拷贝到对应日期分区文件夹，
 *               然后再使用hadoop distcp命令跨级群数据迁移。
 * Version 1.0
 **/
public class CopyFiles2DatePartitionDir {

    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * Hadoop Configuration info
     */
    private Configuration configuration;

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    /**
     * @param src /warehouse/tablespace/managed/hive/dc.db/dc_tb_dx_cd/
     * @param dest 目标文件夹如：/tmp/export/dc_tb_dx_cd/
     * @param partitionPre 分区文件夹前缀如：year_month=
     */
    private void copyFiles(String src, String dest, String partitionPre) {
        Configuration configuration = this.getConfiguration();

        if (dest != null && !("/".equals(dest.substring(dest.length()-1)))) {
            dest = dest + "/";
        }

        try {
            FileSystem fs = FileSystem.get(configuration);
            Path srcPath = new Path(src);

            FileStatus[] statuses = fs.listStatus(srcPath);

            for (FileStatus fileStatus : statuses) {
                // 根据修改时间用FileUtil拷贝文件到指定日期分区文件夹
                long modificationMills = fileStatus.getModificationTime();
                String partitionDate = new SimpleDateFormat("yyyyMM").format(new Date(modificationMills));

                // 拼接目标目录
                String partitionDir = dest + partitionPre + partitionDate;
                Path destPath = new Path(partitionDir);
                if (fs.exists(destPath)) {
                    fs.mkdirs(destPath);
                    logger.info("创建目标文件目录：" + destPath.toString() + " 完成！");
                }

                Path filePath = fileStatus.getPath();
                logger.info("开始拷贝文件：" + filePath.getName() + "====> 文件修改日期为：" +
                        new SimpleDateFormat("yyyy-MM-dd").format(new Date(modificationMills)));
                FileUtil.copy(fs, filePath, fs, destPath, false, configuration);
            }
            logger.info("===========================================================\n拷贝文件完成！");
        } catch (IOException e) {
            logger.error("文件系统获取配置失败！");
            e.printStackTrace();
        }
    }

    /**
     * hadoop jar hadoop-dev-1.0.jar com.wrp.hdfs.CopyFiles2DatePartitionDir
     * /warehouse/tablespace/managed/hive/dc.db/dc_tb_dx_cd/ /tmp/export/dc_tb_dx_cd/ year_month=
     * @param args
     */
    public static void main(String[] args) {
        String src = args[0];
        String dest = args[0];
        String partitionPre = args[0];
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:8020");

        CopyFiles2DatePartitionDir copy = new CopyFiles2DatePartitionDir();
        copy.setConfiguration(conf);
        copy.copyFiles(src, dest, partitionPre);
    }
}
