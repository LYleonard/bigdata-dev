package com.wrp.hdfs;

import org.junit.Test;

/**
 * @ClassName TestFile
 * @Author LYleonard
 * @Date 2020/1/19 14:31
 * @Description TODO
 * Version 1.0
 **/
public class TestFile {
//    public static void main(String[] args) {
//        String path = "file:///usr/local/test.csv";
//        System.out.println(path.substring(0, 4));
//    }

    HdfsJavaAPI hdfsop = new HdfsJavaAPI();

    @Test
    public void testMkdirToHDFS(){
        String directory = "/test/test1";
        hdfsop.mkdirToHdfs(directory);
    }
}
