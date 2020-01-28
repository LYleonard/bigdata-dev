package com.wrp.de.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;

/**
 * @ClassName HdfsJavaAPI
 * @Author LYleonard
 * @Date 2020/1/19 14:11
 * @Description TODO
 * Version 1.0
 **/
public class HdfsJavaAPI {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
//    public static void main(String[] args) {
//
//    }

    /**
     * 在HDFS上创建文件目录
     * @param path: String,新建的文件目录
     * @throws IOException
     */
    public void mkdirToHdfs(String path){
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser("hadoop");
        try {
            userGroupInformation.doAs(new PrivilegedExceptionAction() {
                @Override
                public String run() throws Exception {
                    try {
                        Configuration conf = new Configuration();
                        conf.set("fs.defaultFS","hdfs://192.168.29.100:8020");
                        conf.set("hadoop.job.ugi", "hadoop");

                        FileSystem fileSystem = FileSystem.get(conf);
                        fileSystem.mkdirs(new Path(path));
                        fileSystem.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                        logger.error("Error: ",e);
                    } catch (IllegalArgumentException e) {
                        e.printStackTrace();
                        logger.error("Error: ", e);
                    }
                    return "新建文件目录，操作成功！";
                }
            });
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
            logger.error("Error: ", e);
        }
    }

    public void uploadFile(String src, String dest){
        Configuration conf = new Configuration();

        // 用户名
        String  username = "hadoop";
        // HDFS fs URI
        URI hdfsURI = null;
        try {
            hdfsURI = new URI("hdfs://192.168.29.100:8020");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            logger.error("URISyntaxException: ", e);
        }

        try {
            FileSystem fileSystem = FileSystem.get(hdfsURI, conf, username);
            fileSystem.copyFromLocalFile(new Path(src), new Path(dest));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("IOException: ", e);
        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("InterruptedException: ", e);
        }
    }

    public void downLoadFileFromHdfs(String src, String dest){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.29.100:8020");

        try {
            FileSystem fileSystem = FileSystem.get(conf);
            fileSystem.copyToLocalFile(new Path(src), new Path(dest));
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("IOException: ", e);
        }
    }

    public void deleteHdfsFile(String src, Boolean recursive){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.29.100:8020");
        try {
            FileSystem fs = FileSystem.get(conf);
            fs.delete(new Path(src),recursive);
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("Delete File IOException: ", e);
        }
    }

    public void renameFile(String src, String dest){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.29.100:8020");

        try {
            FileSystem fs = FileSystem.get(conf);
            fs.rename(new Path(src), new Path(dest));
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
            logger.info("Delete File IOException: ", e);
        }
    }

    public void listFiles(String path){
        Configuration conf = new Configuration();
        try {
            // 获取文件系统
            FileSystem fs = FileSystem.get(new URI("hdfs://192.168.29.100:8020"), conf);
            // 获取文件详情
            RemoteIterator<LocatedFileStatus> listFile = fs.listFiles(new Path(path), true);
            while (listFile.hasNext()){
                LocatedFileStatus status = listFile.next();
                // 输出详情
                System.out.println("file name: " + status.getPath().getName());
                System.out.println("Length: " + status.getLen());
                System.out.println("Permission: " + status.getPermission());
                System.out.println("Group: " + status.getGroup());
                BlockLocation[] blockLocations = status.getBlockLocations();
                for (BlockLocation blockLocation: blockLocations){
                    String[] hosts = blockLocation.getHosts();
                    for (String host : hosts){
                        System.out.println(host);
                    }
                }
            }
            fs.close();
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
    }

}
